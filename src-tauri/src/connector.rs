use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use duckdb::params;
use duckdb::Connection;
use serde::Serialize;
use tauri::State;

use crate::catalog;
use crate::error::AppError;
use crate::state::{
    catalog_db_key_for_connector, is_multi_database_connector, normalize_snowflake_database_name,
    AppState, CatalogEntry, ColumnInfo, SINGLE_DB_CATALOG_KEY,
    Connector, ConnectorConfig, ConnectorType, SecondaryAttach,
};

pub trait ConnectorOps {
    fn activate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError>;
    fn deactivate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError>;
    fn introspect(
        &self,
        conn: &Connection,
        connector: &Connector,
    ) -> Result<Vec<CatalogEntry>, AppError>;
    fn test(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError>;
}

pub fn get_ops(ct: &ConnectorType) -> Box<dyn ConnectorOps> {
    match ct {
        ConnectorType::LocalFile => Box::new(FileOps),
        ConnectorType::SQLite => Box::new(SqliteOps),
        ConnectorType::DuckDB => Box::new(DuckDBOps),
        ConnectorType::PostgreSQL => Box::new(PostgresOps),
        ConnectorType::Snowflake => Box::new(SnowflakeOps),
        ConnectorType::S3 | ConnectorType::GCS | ConnectorType::R2 => Box::new(CloudOps),
        ConnectorType::DuckLake => Box::new(DuckLakeOps),
    }
}

fn sanitize_alias(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    let trimmed = sanitized.trim_matches('_').to_lowercase();
    if trimmed.is_empty() {
        "db".to_string()
    } else if trimmed.chars().next().map_or(true, |c| c.is_numeric()) {
        format!("db_{}", trimmed)
    } else {
        trimmed
    }
}

fn make_secondary_attach_alias(connector: &Connector, database: &str) -> String {
    let base = connector.alias.as_deref().unwrap_or("db");
    let id_frag: String = connector
        .id
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .take(8)
        .collect();
    let db_part = sanitize_alias(database);
    let mut s = format!("{}__{}_{}", base, id_frag, db_part);
    const MAX: usize = 115;
    if s.len() > MAX {
        s.truncate(MAX);
    }
    s.trim_end_matches('_').to_string()
}

fn snowflake_secondary_secret_name(connector: &Connector, database: &str) -> String {
    let db_part = sanitize_alias(database);
    let short_id: String = connector.id.chars().filter(|c| *c != '-').take(10).collect();
    format!("cdv_sf_{}_{}", short_id, db_part)
}

fn build_snowflake_secret_sql(connector: &Connector, database: &str, secret_name: &str) -> Result<String, AppError> {
    let cfg = &connector.config;
    let raw_host = cfg
        .host
        .as_deref()
        .ok_or_else(|| AppError::ConnectorError("Snowflake connector missing account".into()))?;
    let account = extract_snowflake_account(raw_host);

    let mut secret_parts = vec![
        "TYPE snowflake".to_string(),
        format!("ACCOUNT '{}'", escape_sql_string(account)),
        format!("DATABASE '{}'", escape_sql_string(database)),
    ];
    if let Some(user) = cfg.user.as_deref().filter(|s| !s.is_empty()) {
        secret_parts.push(format!("USER '{}'", escape_sql_string(user)));
    }
    if let Some(password) = cfg.password.as_deref().filter(|s| !s.is_empty()) {
        secret_parts.push(format!("PASSWORD '{}'", escape_sql_string(password)));
    }
    if let Some(wh) = cfg.warehouse.as_deref().filter(|s| !s.is_empty()) {
        secret_parts.push(format!("WAREHOUSE '{}'", escape_sql_string(wh)));
    }

    Ok(format!(
        "CREATE PERSISTENT SECRET \"{}\" ({})",
        secret_name,
        secret_parts.join(", ")
    ))
}

fn attach_snowflake_with_secret(
    conn: &Connection,
    attach_alias: &str,
    secret_name: &str,
) -> Result<(), AppError> {
    let attach_sql = format!(
        "ATTACH '' AS \"{}\" (TYPE snowflake, SECRET \"{}\", READ_ONLY)",
        attach_alias.replace('"', ""),
        secret_name.replace('"', "")
    );
    conn.execute_batch(&attach_sql).map_err(|e| {
        AppError::ConnectorError(format!("Failed to attach Snowflake database: {}", e))
    })?;
    Ok(())
}

fn attach_postgres_database(
    conn: &Connection,
    connector: &Connector,
    database: &str,
    attach_alias: &str,
) -> Result<(), AppError> {
    ensure_extension(conn, "postgres")?;
    let mut cfg = connector.config.clone();
    cfg.database = Some(database.to_string());
    let connstr = build_pg_connstr(&cfg)?;
    let safe_alias = attach_alias.replace('"', "");
    let sql = format!(
        "ATTACH '{}' AS \"{}\" (TYPE POSTGRES, READ_ONLY)",
        escape_sql_string(&connstr),
        safe_alias
    );
    conn.execute_batch(&sql)
        .map_err(|e| AppError::ConnectorError(format!("Failed to attach PostgreSQL: {}", e)))?;
    Ok(())
}

fn detach_alias(conn: &Connection, alias: &str) {
    let safe = alias.replace('"', "");
    conn.execute_batch(&format!("DETACH \"{}\"", safe)).ok();
}

fn drop_secret_if_exists(conn: &Connection, name: &str) {
    let safe = name.replace('"', "");
    conn.execute_batch(&format!("DROP PERSISTENT SECRET IF EXISTS \"{}\"", safe)).ok();
    conn.execute_batch(&format!("DROP SECRET IF EXISTS \"{}\"", safe)).ok();
}

fn detach_secondary_attaches(conn: &Connection, secondaries: &[SecondaryAttach]) {
    for s in secondaries {
        detach_alias(conn, &s.attach_alias);
        if let Some(sn) = &s.secret_name {
            drop_secret_if_exists(conn, sn);
        }
    }
}

/// Resolve DuckDB attach alias for import/query (`"alias"."schema"."table"`).
pub(crate) fn resolve_attach_alias_for_database(
    connector: &Connector,
    database: Option<&str>,
    secondaries: &[SecondaryAttach],
) -> Result<String, AppError> {
    let base_alias = connector
        .alias
        .as_deref()
        .ok_or_else(|| AppError::ConnectorError("Database connector missing alias".into()))?;
    let default_key = catalog_db_key_for_connector(connector);
    let db_owned: String = if let Some(d) = database.filter(|s| !s.is_empty()) {
        if matches!(connector.connector_type, ConnectorType::Snowflake) {
            normalize_snowflake_database_name(d)
        } else {
            d.to_string()
        }
    } else {
        default_key.clone()
    };
    let db = db_owned.as_str();
    if db == default_key.as_str() {
        return Ok(base_alias.to_string());
    }
    for s in secondaries {
        let matches = if matches!(connector.connector_type, ConnectorType::Snowflake) {
            normalize_snowflake_database_name(&s.database) == db_owned
        } else {
            s.database == db_owned
        };
        if matches {
            return Ok(s.attach_alias.clone());
        }
    }
    Err(AppError::ConnectorError(format!(
        "Database '{}' is not connected for this connector",
        db
    )))
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ConnectorBrowseCache {
    #[serde(default)]
    pub database_names: Vec<String>,
    pub default_database: String,
    #[serde(default)]
    pub catalogs_by_database: HashMap<String, Vec<CatalogEntry>>,
    /// DuckDB attach alias per database name (for building qualified SQL in the UI).
    #[serde(default)]
    pub attach_aliases_by_database: HashMap<String, String>,
}

pub fn rehydrate_secondary_attaches(
    conn: &Connection,
    secondary_map: &HashMap<String, Vec<SecondaryAttach>>,
    connectors: &[Connector],
) {
    for (id, attaches) in secondary_map {
        let Some(connector) = connectors.iter().find(|c| &c.id == id) else {
            continue;
        };
        if !matches!(
            connector.connector_type,
            ConnectorType::PostgreSQL | ConnectorType::Snowflake
        ) {
            continue;
        }
        for s in attaches {
            match connector.connector_type {
                ConnectorType::PostgreSQL => {
                    if let Err(e) =
                        attach_postgres_database(conn, connector, &s.database, &s.attach_alias)
                    {
                        eprintln!(
                            "Rehydrate PG secondary '{}' / {}: {}",
                            connector.name, s.database, e
                        );
                    }
                }
                ConnectorType::Snowflake => {
                    let secret_name = s.secret_name.clone().unwrap_or_else(|| {
                        snowflake_secondary_secret_name(connector, &s.database)
                    });
                    if connector.config.password.as_deref().filter(|p| !p.is_empty()).is_some() {
                        if let Ok(sql) =
                            build_snowflake_secret_sql(connector, &s.database, &secret_name)
                        {
                            conn.execute_batch(&sql).ok();
                        }
                    }
                    if let Err(e) =
                        attach_snowflake_with_secret(conn, &s.attach_alias, &secret_name)
                    {
                        eprintln!(
                            "Rehydrate Snowflake secondary '{}' / {}: {}",
                            connector.name, s.database, e
                        );
                    }
                }
                _ => {}
            }
        }
    }
}

pub(crate) fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn is_url(path: &str) -> bool {
    path.starts_with("http://") || path.starts_with("https://")
}

pub(crate) fn ensure_extension(conn: &Connection, ext: &str) -> Result<(), AppError> {
    conn.execute_batch(&format!("INSTALL '{}'; LOAD '{}';", ext, ext))
        .map_err(|e| {
            AppError::ConnectorError(format!("Failed to load {} extension: {}", ext, e))
        })
}

fn ensure_community_extension(conn: &Connection, ext: &str) -> Result<(), AppError> {
    conn.execute_batch(&format!("INSTALL {} FROM community; LOAD {};", ext, ext))
        .map_err(|e| {
            AppError::ConnectorError(format!(
                "Failed to load {} community extension: {}",
                ext, e
            ))
        })
}

fn describe_ref(conn: &Connection, duckdb_ref: &str) -> Result<Vec<ColumnInfo>, AppError> {
    let sql = format!("DESCRIBE SELECT * FROM {}", duckdb_ref);
    let mut stmt = conn.prepare(&sql)?;
    let schema: Vec<ColumnInfo> = stmt
        .query_map(params![], |row| {
            let key: Option<String> = row.get(3).ok();
            let key = key.filter(|s| !s.is_empty());
            Ok(ColumnInfo {
                name: row.get(0)?,
                data_type: row.get(1)?,
                nullable: {
                    let val: String = row.get(2)?;
                    val == "YES"
                },
                key,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(schema)
}

fn count_ref(conn: &Connection, duckdb_ref: &str) -> Option<u64> {
    let sql = format!("SELECT COUNT(*) FROM {}", duckdb_ref);
    conn.query_row(&sql, params![], |row| row.get(0)).ok()
}

// ──────────────────────────── FileOps ────────────────────────────

pub struct FileOps;

impl ConnectorOps for FileOps {
    fn activate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        let path = connector
            .config
            .path
            .as_deref()
            .unwrap_or("");
        if is_url(path) {
            ensure_extension(conn, "httpfs")?;
        }
        Ok(())
    }

    fn deactivate(&self, _conn: &Connection, _connector: &Connector) -> Result<(), AppError> {
        Ok(())
    }

    fn introspect(
        &self,
        conn: &Connection,
        connector: &Connector,
    ) -> Result<Vec<CatalogEntry>, AppError> {
        let path = connector
            .config
            .path
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("File connector missing path".into()))?;
        let format = connector
            .config
            .format
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("File connector missing format".into()))?;

        let (_, duckdb_ref) = crate::dataset_manager::format_to_ref(path, format)?;
        let columns = describe_ref(conn, &duckdb_ref).unwrap_or_default();
        let row_count = count_ref(conn, &duckdb_ref);

        let name = Path::new(path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("untitled")
            .to_string();

        Ok(vec![CatalogEntry {
            schema: None,
            name,
            entry_type: "file".to_string(),
            columns,
            row_count,
        }])
    }

    fn test(&self, _conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        let path = connector
            .config
            .path
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("File connector missing path".into()))?;
        if path.starts_with("s3://") || path.starts_with("gcs://") || is_url(path)
        {
            return Ok(());
        }
        if !Path::new(path).exists() {
            return Err(AppError::ConnectorError(format!(
                "File not found: {}",
                path
            )));
        }
        Ok(())
    }
}

// ──────────────────────────── CloudOps ────────────────────────────

pub struct CloudOps;

fn cloud_path_scheme(ct: &ConnectorType) -> &'static str {
    match ct {
        ConnectorType::GCS => "gcs",
        _ => "s3",
    }
}

fn build_secret_sql(
    secret_name: &str,
    connector: &Connector,
    access_key: &str,
    secret_key: &str,
) -> Result<String, AppError> {
    let cfg = &connector.config;
    let bucket = cfg
        .bucket
        .as_deref()
        .ok_or_else(|| AppError::ConnectorError("Cloud connector missing bucket".into()))?;
    let region = cfg.region.as_deref().unwrap_or("us-east-1");

    let parts: Vec<String> = match &connector.connector_type {
        ConnectorType::GCS => {
            let scope = format!("gcs://{}", bucket);
            vec![
                "TYPE GCS".to_string(),
                format!("KEY_ID '{}'", access_key),
                format!("SECRET '{}'", secret_key),
                format!("SCOPE '{}'", scope),
            ]
        }
        ConnectorType::R2 => {
            let account = cfg
                .account_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AppError::ConnectorError("Cloudflare R2 requires Account ID".into())
                })?;
            vec![
                "TYPE R2".to_string(),
                format!("ACCOUNT_ID '{}'", account),
                format!("KEY_ID '{}'", access_key),
                format!("SECRET '{}'", secret_key),
                format!(
                    "REGION '{}'",
                    if region.is_empty() { "auto" } else { region }
                ),
            ]
        }
        _ => {
            let scope = format!("s3://{}", bucket);
            let mut parts = vec![
                "TYPE S3".to_string(),
                format!("KEY_ID '{}'", access_key),
                format!("SECRET '{}'", secret_key),
                format!(
                    "REGION '{}'",
                    if region.is_empty() { "us-east-1" } else { region }
                ),
                format!("SCOPE '{}'", scope),
            ];
            if let Some(ep) = &cfg.endpoint {
                if !ep.is_empty() {
                    parts.push(format!("ENDPOINT '{}'", ep));
                    parts.push("URL_STYLE 'path'".to_string());
                }
            }
            parts
        }
    };

    Ok(format!(
        "CREATE PERSISTENT SECRET \"{}\" ({})",
        secret_name,
        parts.join(", ")
    ))
}

impl ConnectorOps for CloudOps {
    fn activate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        ensure_extension(conn, "httpfs")?;

        let secret_name = connector
            .secret_name
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Cloud connector missing secret_name".into()))?;

        let access_key = connector
            .config
            .user
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Cloud connector missing access key".into()))?;
        let secret_key = connector
            .config
            .password
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Cloud connector missing secret key".into()))?;

        let sql = build_secret_sql(secret_name, connector, access_key, secret_key)?;
        conn.execute_batch(&sql)
            .map_err(|e| AppError::ConnectorError(format!("Failed to create secret: {}", e)))?;

        Ok(())
    }

    fn deactivate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        if let Some(secret_name) = &connector.secret_name {
            drop_secret_if_exists(conn, secret_name);
        }
        Ok(())
    }

    fn introspect(
        &self,
        conn: &Connection,
        connector: &Connector,
    ) -> Result<Vec<CatalogEntry>, AppError> {
        let cfg = &connector.config;
        let bucket = cfg
            .bucket
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Cloud connector missing bucket".into()))?;
        let scheme = cloud_path_scheme(&connector.connector_type);
        let path = match &cfg.prefix {
            Some(p) if !p.is_empty() => format!("{}://{}/{}*", scheme, bucket, p),
            _ => format!("{}://{}/*", scheme, bucket),
        };

        let sql = format!("SELECT file FROM glob('{}')", path);
        let mut stmt = conn.prepare(&sql)?;
        let files: Vec<String> = stmt
            .query_map(params![], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(files
            .into_iter()
            .map(|f| CatalogEntry {
                schema: None,
                name: f,
                entry_type: "file".to_string(),
                columns: vec![],
                row_count: None,
            })
            .collect())
    }

    fn test(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        self.activate(conn, connector)?;
        let result = self.introspect(conn, connector);
        self.deactivate(conn, connector).ok();
        result.map(|_| ())
    }
}

// ──────────────────────────── SqliteOps ────────────────────────────

pub struct SqliteOps;

impl ConnectorOps for SqliteOps {
    fn activate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        ensure_extension(conn, "sqlite")?;
        let path = connector
            .config
            .path
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("SQLite connector missing path".into()))?;
        if is_url(path) {
            ensure_extension(conn, "httpfs")?;
        }
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("SQLite connector missing alias".into()))?;
        let sql = format!(
            "ATTACH '{}' AS \"{}\" (TYPE SQLITE, READ_ONLY)",
            escape_sql_string(path),
            alias
        );
        conn.execute_batch(&sql)
            .map_err(|e| AppError::ConnectorError(format!("Failed to attach SQLite DB: {}", e)))?;
        Ok(())
    }

    fn deactivate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        if let Some(alias) = &connector.alias {
            conn.execute_batch(&format!("DETACH \"{}\"", alias))
                .map_err(|e| {
                    AppError::ConnectorError(format!("Failed to detach SQLite DB: {}", e))
                })?;
        }
        Ok(())
    }

    fn introspect(
        &self,
        conn: &Connection,
        connector: &Connector,
    ) -> Result<Vec<CatalogEntry>, AppError> {
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("SQLite connector missing alias".into()))?;
        introspect_attached_db(conn, alias)
    }

    fn test(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        let path = connector
            .config
            .path
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("SQLite connector missing path".into()))?;
        if !is_url(path) && !Path::new(path).exists() {
            return Err(AppError::ConnectorError(format!(
                "SQLite file not found: {}",
                path
            )));
        }
        self.activate(conn, connector)?;
        self.deactivate(conn, connector).ok();
        Ok(())
    }
}

// ──────────────────────────── DuckDBOps ────────────────────────────

pub struct DuckDBOps;

impl ConnectorOps for DuckDBOps {
    fn activate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        let path = connector
            .config
            .path
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("DuckDB connector missing path".into()))?;
        if is_url(path) {
            ensure_extension(conn, "httpfs")?;
        }
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("DuckDB connector missing alias".into()))?;
        let sql = format!(
            "ATTACH '{}' AS \"{}\"",
            escape_sql_string(path),
            alias
        );
        conn.execute_batch(&sql)
            .map_err(|e| AppError::ConnectorError(format!("Failed to attach DuckDB: {}", e)))?;
        Ok(())
    }

    fn deactivate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        if let Some(alias) = &connector.alias {
            conn.execute_batch(&format!("DETACH \"{}\"", alias))
                .map_err(|e| {
                    AppError::ConnectorError(format!("Failed to detach DuckDB: {}", e))
                })?;
        }
        Ok(())
    }

    fn introspect(
        &self,
        conn: &Connection,
        connector: &Connector,
    ) -> Result<Vec<CatalogEntry>, AppError> {
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("DuckDB connector missing alias".into()))?;
        introspect_attached_duckdb(conn, alias)
    }

    fn test(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        let path = connector
            .config
            .path
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("DuckDB connector missing path".into()))?;
        if !is_url(path) && !Path::new(path).exists() {
            return Err(AppError::ConnectorError(format!(
                "DuckDB file not found: {}",
                path
            )));
        }
        self.activate(conn, connector)?;
        self.deactivate(conn, connector).ok();
        Ok(())
    }
}

// ──────────────────────────── PostgresOps ────────────────────────────

pub struct PostgresOps;

fn build_pg_connstr(cfg: &ConnectorConfig) -> Result<String, AppError> {
    let host = cfg
        .host
        .as_deref()
        .ok_or_else(|| AppError::ConnectorError("PostgreSQL connector missing host".into()))?;
    let port = cfg.port.unwrap_or(5432);
    let database = cfg
        .database
        .as_deref()
        .ok_or_else(|| AppError::ConnectorError("PostgreSQL connector missing database".into()))?;

    let mut connstr = format!("host={} port={} dbname={}", host, port, database);
    if let Some(user) = cfg.user.as_deref().filter(|s| !s.is_empty()) {
        connstr.push_str(&format!(" user={}", user));
    }
    if let Some(password) = cfg.password.as_deref().filter(|s| !s.is_empty()) {
        connstr.push_str(&format!(" password={}", password));
    }
    Ok(connstr)
}

impl ConnectorOps for PostgresOps {
    fn activate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        ensure_extension(conn, "postgres")?;
        let connstr = build_pg_connstr(&connector.config)?;
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("PostgreSQL connector missing alias".into()))?;
        let sql = format!(
            "ATTACH '{}' AS \"{}\" (TYPE POSTGRES, READ_ONLY)",
            escape_sql_string(&connstr),
            alias
        );
        conn.execute_batch(&sql)
            .map_err(|e| AppError::ConnectorError(format!("Failed to attach PostgreSQL: {}", e)))?;
        Ok(())
    }

    fn deactivate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        if let Some(alias) = &connector.alias {
            conn.execute_batch(&format!("DETACH \"{}\"", alias))
                .map_err(|e| {
                    AppError::ConnectorError(format!("Failed to detach PostgreSQL: {}", e))
                })?;
        }
        Ok(())
    }

    fn introspect(
        &self,
        conn: &Connection,
        connector: &Connector,
    ) -> Result<Vec<CatalogEntry>, AppError> {
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| {
                AppError::ConnectorError("PostgreSQL connector missing alias".into())
            })?;
        introspect_attached_postgres(conn, alias)
    }

    fn test(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        self.activate(conn, connector)?;
        self.deactivate(conn, connector).ok();
        Ok(())
    }
}

// ──────────────────────────── Snowflake ADBC driver auto-install ────────────────────────────

const ADBC_RELEASE_TAG: &str = "apache-arrow-adbc-20";
const ADBC_DRIVER_VERSION: &str = "1.8.0";

fn detect_adbc_platform() -> Result<(&'static str, &'static str), AppError> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    match (os, arch) {
        ("linux", "x86_64") => Ok((
            "linux_amd64",
            "adbc_driver_snowflake-1.8.0-py3-none-manylinux1_x86_64.manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_5_x86_64.whl",
        )),
        ("linux", "aarch64") => Ok((
            "linux_arm64",
            "adbc_driver_snowflake-1.8.0-py3-none-manylinux2014_aarch64.manylinux_2_17_aarch64.whl",
        )),
        ("macos", "x86_64") => Ok((
            "osx_amd64",
            "adbc_driver_snowflake-1.8.0-py3-none-macosx_10_15_x86_64.whl",
        )),
        ("macos", "aarch64") => Ok((
            "osx_arm64",
            "adbc_driver_snowflake-1.8.0-py3-none-macosx_11_0_arm64.whl",
        )),
        ("windows", "x86_64") => Ok((
            "windows_amd64",
            "adbc_driver_snowflake-1.8.0-py3-none-win_amd64.whl",
        )),
        _ => Err(AppError::ConnectorError(format!(
            "Unsupported platform for Snowflake ADBC driver: {} {}",
            os, arch
        ))),
    }
}

fn get_duckdb_version(conn: &Connection) -> Result<String, AppError> {
    let version: String = conn
        .query_row(
            "SELECT library_version FROM pragma_version()",
            params![],
            |row| row.get(0),
        )
        .map_err(|e| AppError::ConnectorError(format!("Failed to get DuckDB version: {}", e)))?;
    if version.starts_with('v') {
        Ok(version)
    } else {
        Ok(format!("v{}", version))
    }
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

/// Checks whether the Snowflake ADBC driver library is present in the DuckDB
/// extensions directory. If missing, downloads the appropriate wheel from the
/// Apache Arrow ADBC GitHub release and extracts the shared library.
fn ensure_adbc_snowflake_driver(conn: &Connection) -> Result<(), AppError> {
    let (platform, wheel_name) = detect_adbc_platform()?;
    let version = get_duckdb_version(conn)?;

    let home = home_dir()
        .ok_or_else(|| AppError::ConnectorError("Cannot determine home directory".into()))?;
    let ext_dir = home
        .join(".duckdb")
        .join("extensions")
        .join(&version)
        .join(platform);

    let lib_filename = if cfg!(windows) {
        "adbc_driver_snowflake.dll"
    } else {
        "libadbc_driver_snowflake.so"
    };
    let driver_path = ext_dir.join(lib_filename);

    if driver_path.exists() {
        return Ok(());
    }

    eprintln!(
        "Snowflake ADBC driver not found at {}; downloading v{}…",
        driver_path.display(),
        ADBC_DRIVER_VERSION
    );

    std::fs::create_dir_all(&ext_dir).map_err(|e| {
        AppError::ConnectorError(format!("Failed to create extensions directory: {}", e))
    })?;

    let url = format!(
        "https://github.com/apache/arrow-adbc/releases/download/{}/{}",
        ADBC_RELEASE_TAG, wheel_name
    );

    let response = ureq::get(&url).call().map_err(|e| {
        AppError::ConnectorError(format!(
            "Failed to download Snowflake ADBC driver from {}: {}",
            url, e
        ))
    })?;

    let mut body = Vec::new();
    response
        .into_reader()
        .take(200_000_000) // 200 MB safety limit
        .read_to_end(&mut body)
        .map_err(|e| {
            AppError::ConnectorError(format!("Failed to read ADBC driver download: {}", e))
        })?;

    let cursor = std::io::Cursor::new(&body);
    let mut archive = zip::ZipArchive::new(cursor).map_err(|e| {
        AppError::ConnectorError(format!("Failed to open ADBC driver archive: {}", e))
    })?;

    let inner_path = if cfg!(windows) {
        "adbc_driver_snowflake/adbc_driver_snowflake.dll"
    } else {
        "adbc_driver_snowflake/libadbc_driver_snowflake.so"
    };

    let mut entry = archive.by_name(inner_path).map_err(|e| {
        AppError::ConnectorError(format!(
            "ADBC driver library not found in downloaded archive: {}",
            e
        ))
    })?;

    let mut lib_bytes = Vec::new();
    entry.read_to_end(&mut lib_bytes).map_err(|e| {
        AppError::ConnectorError(format!("Failed to extract ADBC driver from archive: {}", e))
    })?;
    drop(entry);

    std::fs::write(&driver_path, &lib_bytes).map_err(|e| {
        AppError::ConnectorError(format!("Failed to write ADBC driver to disk: {}", e))
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&driver_path, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| {
                AppError::ConnectorError(format!("Failed to set ADBC driver permissions: {}", e))
            })?;
    }

    eprintln!(
        "Snowflake ADBC driver v{} installed to {}",
        ADBC_DRIVER_VERSION,
        driver_path.display()
    );

    Ok(())
}

// ──────────────────────────── SnowflakeOps ────────────────────────────

pub struct SnowflakeOps;

/// DuckDB secret name for the connector's default Snowflake database.
/// Must be stable across app restarts: passwords are not serialized, but the secret
/// can remain inside `cdv.duckdb`, so reload must use the same name as `add_connector`
/// (`cdv_{id}`), not a name derived only from alias (older fallback broke rehydrate).
fn snowflake_secret_name(connector: &Connector) -> String {
    connector.secret_name.clone().unwrap_or_else(|| {
        format!(
            "cdv_{}",
            connector.id.replace('-', "_")
        )
    })
}

/// Older builds fell back to this when `secret_name` was missing after deserialize.
fn snowflake_legacy_secret_name(connector: &Connector) -> String {
    format!(
        "cdv_sf_{}",
        connector.alias.as_deref().unwrap_or("sf")
    )
}

fn extract_snowflake_account(host: &str) -> &str {
    host.strip_suffix(".snowflakecomputing.com")
        .unwrap_or(host)
}

impl ConnectorOps for SnowflakeOps {
    fn activate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        ensure_adbc_snowflake_driver(conn)?;
        ensure_community_extension(conn, "snowflake")?;

        let cfg = &connector.config;
        let raw_host = cfg
            .host
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Snowflake connector missing account".into()))?;
        let account = extract_snowflake_account(raw_host);
        let database = cfg
            .database
            .as_deref()
            .ok_or_else(|| {
                AppError::ConnectorError("Snowflake connector missing database".into())
            })?;
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Snowflake connector missing alias".into()))?;

        let secret_name = snowflake_secret_name(connector);
        let has_password = cfg
            .password
            .as_deref()
            .map(|s| !s.is_empty())
            .unwrap_or(false);

        if has_password {
            detach_alias(conn, alias);
            drop_secret_if_exists(conn, &secret_name);

            let mut secret_parts = vec![
                "TYPE snowflake".to_string(),
                format!("ACCOUNT '{}'", escape_sql_string(account)),
                format!("DATABASE '{}'", escape_sql_string(database)),
            ];
            if let Some(user) = cfg.user.as_deref().filter(|s| !s.is_empty()) {
                secret_parts.push(format!("USER '{}'", escape_sql_string(user)));
            }
            let pw = cfg
                .password
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AppError::ConnectorError("Snowflake connector missing password".into())
                })?;
            secret_parts.push(format!("PASSWORD '{}'", escape_sql_string(pw)));
            if let Some(wh) = cfg.warehouse.as_deref().filter(|s| !s.is_empty()) {
                secret_parts.push(format!("WAREHOUSE '{}'", escape_sql_string(wh)));
            }

            let secret_sql = format!(
                "CREATE PERSISTENT SECRET \"{}\" ({})",
                secret_name.replace('"', ""),
                secret_parts.join(", ")
            );
            conn.execute_batch(&secret_sql).map_err(|e| {
                AppError::ConnectorError(format!("Failed to create Snowflake secret: {}", e))
            })?;

            attach_snowflake_with_secret(conn, alias, &secret_name)?;
            return Ok(());
        }

        // No password in memory (normal after restart: passwords are not persisted). The secret
        // may still exist inside the persistent DuckDB file from the last session—attach only.
        detach_alias(conn, alias);
        let attach_err = match attach_snowflake_with_secret(conn, alias, &secret_name) {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };

        let legacy = snowflake_legacy_secret_name(connector);
        if legacy != secret_name {
            if attach_snowflake_with_secret(conn, alias, &legacy).is_ok() {
                return Ok(());
            }
        }

        Err(AppError::ConnectorError(format!(
            "Snowflake: credentials are not in memory (passwords are never saved to catalog.json). \
             Could not re-use a stored DuckDB secret—often after deleting cdv.duckdb or on a new machine. \
             Open the connection, enter your password again, and connect. \
             Detail: {}",
            attach_err
        )))
    }

    fn deactivate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        if let Some(alias) = &connector.alias {
            conn.execute_batch(&format!("DETACH \"{}\"", alias))
                .map_err(|e| {
                    AppError::ConnectorError(format!("Failed to detach Snowflake: {}", e))
                })?;
        }
        let secret_name = snowflake_secret_name(connector);
        drop_secret_if_exists(conn, &secret_name);
        Ok(())
    }

    fn introspect(
        &self,
        conn: &Connection,
        connector: &Connector,
    ) -> Result<Vec<CatalogEntry>, AppError> {
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| {
                AppError::ConnectorError("Snowflake connector missing alias".into())
            })?;
        let database = connector
            .config
            .database
            .as_deref()
            .unwrap_or("");
        introspect_attached_snowflake(conn, alias, database)
    }

    fn test(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        self.activate(conn, connector)?;
        self.deactivate(conn, connector).ok();
        Ok(())
    }
}

// ──────────────────────────── DuckLakeOps ────────────────────────────

pub struct DuckLakeOps;

impl ConnectorOps for DuckLakeOps {
    fn activate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        ensure_extension(conn, "ducklake")?;

        let catalog_type = connector
            .config
            .catalog_type
            .as_deref()
            .unwrap_or("duckdb");
        match catalog_type {
            "postgres" => {
                ensure_extension(conn, "postgres")?;
            }
            "sqlite" => {
                ensure_extension(conn, "sqlite")?;
            }
            _ => {}
        }

        if let Some(dp) = &connector.config.data_path {
            if dp.starts_with("s3://") || dp.starts_with("gcs://") {
                ensure_extension(conn, "httpfs")?;
            }
        }

        let metadata_path = connector
            .config
            .metadata_path
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("DuckLake missing metadata path".into()))?;
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("DuckLake missing alias".into()))?;

        let mut params = Vec::new();
        if let Some(dp) = &connector.config.data_path {
            if !dp.is_empty() {
                params.push(format!("DATA_PATH '{}'", escape_sql_string(dp)));
                params.push("OVERRIDE_DATA_PATH true".to_string());
            }
        }
        if connector.config.read_only.unwrap_or(false) {
            params.push("READ_ONLY".to_string());
        }

        let sql = if params.is_empty() {
            format!(
                "ATTACH 'ducklake:{}' AS \"{}\"",
                escape_sql_string(metadata_path),
                alias
            )
        } else {
            format!(
                "ATTACH 'ducklake:{}' AS \"{}\" ({})",
                escape_sql_string(metadata_path),
                alias,
                params.join(", ")
            )
        };

        conn.execute_batch(&sql)
            .map_err(|e| AppError::ConnectorError(format!("Failed to attach DuckLake: {}", e)))?;
        Ok(())
    }

    fn deactivate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        if let Some(alias) = &connector.alias {
            conn.execute_batch(&format!("DETACH \"{}\"", alias))
                .map_err(|e| {
                    AppError::ConnectorError(format!("Failed to detach DuckLake: {}", e))
                })?;
        }
        Ok(())
    }

    fn introspect(
        &self,
        conn: &Connection,
        connector: &Connector,
    ) -> Result<Vec<CatalogEntry>, AppError> {
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("DuckLake missing alias".into()))?;
        introspect_attached_duckdb(conn, alias)
    }

    fn test(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        self.activate(conn, connector)?;
        self.deactivate(conn, connector).ok();
        Ok(())
    }
}

// ──────────────────────────── Shared introspection for ATTACH'd databases ────────────────────────────

/// Introspect an attached DuckDB file using duckdb_tables() and duckdb_views().
/// Attached DuckDB databases do not expose information_schema.tables in a way we can
/// query from the parent connection, so we use the metadata table functions instead.
fn introspect_attached_duckdb(
    conn: &Connection,
    alias: &str,
) -> Result<Vec<CatalogEntry>, AppError> {
    struct TableInfo {
        schema: String,
        name: String,
        is_view: bool,
    }

    let mut items: Vec<TableInfo> = Vec::new();

    let tables_sql = format!(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = '{}'",
        escape_sql_string(alias)
    );
    if let Ok(mut stmt) = conn.prepare(&tables_sql) {
        let rows: Vec<TableInfo> = stmt
            .query_map(params![], |row| {
                Ok(TableInfo {
                    schema: row.get(0)?,
                    name: row.get(1)?,
                    is_view: false,
                })
            })
            .map(|mapped| mapped.filter_map(|r| r.ok()).collect())
            .unwrap_or_default();
        items.extend(rows);
    }

    let views_sql = format!(
        "SELECT schema_name, view_name FROM duckdb_views() WHERE database_name = '{}' AND NOT internal",
        escape_sql_string(alias)
    );
    if let Ok(mut stmt) = conn.prepare(&views_sql) {
        let rows: Vec<TableInfo> = stmt
            .query_map(params![], |row| {
                Ok(TableInfo {
                    schema: row.get(0)?,
                    name: row.get(1)?,
                    is_view: true,
                })
            })
            .map(|mapped| mapped.filter_map(|r| r.ok()).collect())
            .unwrap_or_default();
        items.extend(rows);
    }

    let mut entries = Vec::with_capacity(items.len());
    for t in &items {
        let qualified = format!("\"{}\".\"{}\".\"{}\"", alias, t.schema, t.name);
        let columns = describe_ref(conn, &qualified).unwrap_or_default();
        let row_count = count_ref(conn, &qualified);
        let entry_type = if t.is_view { "view" } else { "table" };
        entries.push(CatalogEntry {
            schema: Some(t.schema.clone()),
            name: t.name.clone(),
            entry_type: entry_type.to_string(),
            columns,
            row_count,
        });
    }

    Ok(entries)
}

/// Postgres-specific introspection that fetches all metadata in bulk:
/// - Table list from information_schema.tables (1 query)
/// - All column info from information_schema.columns (1 query)
/// - Approximate row counts from pg_catalog.pg_class (1 query)
/// Total: 3 network round trips regardless of table count.
fn introspect_attached_postgres(
    conn: &Connection,
    alias: &str,
) -> Result<Vec<CatalogEntry>, AppError> {
    let tables_sql = format!(
        "SELECT table_schema, table_name, table_type \
         FROM information_schema.tables \
         WHERE table_catalog = '{}'",
        escape_sql_string(alias)
    );
    let mut stmt = conn.prepare(&tables_sql)?;

    struct TableInfo {
        schema: String,
        name: String,
        table_type: String,
    }

    let tables: Vec<TableInfo> = stmt
        .query_map(params![], |row| {
            Ok(TableInfo {
                schema: row.get(0)?,
                name: row.get(1)?,
                table_type: row.get(2)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Bulk-fetch all column metadata in one query instead of N DESCRIBE calls
    let columns_sql = format!(
        "SELECT table_schema, table_name, column_name, data_type, is_nullable \
         FROM information_schema.columns \
         WHERE table_catalog = '{}' \
         ORDER BY table_schema, table_name, ordinal_position",
        escape_sql_string(alias)
    );

    let mut columns_map: std::collections::HashMap<(String, String), Vec<ColumnInfo>> =
        std::collections::HashMap::new();
    if let Ok(mut col_stmt) = conn.prepare(&columns_sql) {
        if let Ok(rows) = col_stmt.query_map(params![], |row| {
            let schema: String = row.get(0)?;
            let table: String = row.get(1)?;
            let col_name: String = row.get(2)?;
            let data_type: String = row.get(3)?;
            let nullable_str: String = row.get(4)?;
            Ok((schema, table, col_name, data_type, nullable_str))
        }) {
            for r in rows.flatten() {
                let key = (r.0, r.1);
                columns_map.entry(key).or_default().push(ColumnInfo {
                    name: r.2,
                    data_type: r.3,
                    nullable: r.4 == "YES",
                    key: None,
                });
            }
        }
    }

    // Bulk-fetch primary key info from pg_catalog
    let pk_sql = format!(
        "SELECT kcu.table_schema, kcu.table_name, kcu.column_name \
         FROM \"{alias}\".information_schema.table_constraints tc \
         JOIN \"{alias}\".information_schema.key_column_usage kcu \
           ON tc.constraint_name = kcu.constraint_name \
           AND tc.table_schema = kcu.table_schema \
         WHERE tc.constraint_type = 'PRIMARY KEY'"
    );

    let mut pk_set: std::collections::HashSet<(String, String, String)> =
        std::collections::HashSet::new();
    if let Ok(mut pk_stmt) = conn.prepare(&pk_sql) {
        if let Ok(rows) = pk_stmt.query_map(params![], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        }) {
            for r in rows.flatten() {
                pk_set.insert(r);
            }
        }
    }

    // Apply PK info to columns
    for ((schema, table), cols) in &mut columns_map {
        for col in cols.iter_mut() {
            if pk_set.contains(&(schema.clone(), table.clone(), col.name.clone())) {
                col.key = Some("PRI".to_string());
            }
        }
    }

    // Bulk-fetch approximate row counts from pg_catalog
    let counts_sql = format!(
        "SELECT n.nspname, c.relname, c.reltuples::BIGINT \
         FROM \"{alias}\".pg_catalog.pg_class c \
         JOIN \"{alias}\".pg_catalog.pg_namespace n \
           ON c.relnamespace = n.oid \
         WHERE c.relkind IN ('r', 'v', 'm', 'p')"
    );

    let mut approx_counts: std::collections::HashMap<(String, String), i64> =
        std::collections::HashMap::new();
    if let Ok(mut count_stmt) = conn.prepare(&counts_sql) {
        if let Ok(rows) = count_stmt.query_map(params![], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        }) {
            for r in rows.flatten() {
                approx_counts.insert((r.0, r.1), r.2);
            }
        }
    }

    let mut entries = Vec::with_capacity(tables.len());
    for t in &tables {
        let key = (t.schema.clone(), t.name.clone());
        let columns = columns_map.remove(&key).unwrap_or_default();
        let row_count = approx_counts
            .get(&key)
            .and_then(|&c| if c < 0 { None } else { Some(c as u64) });
        let entry_type = if t.table_type.contains("VIEW") {
            "view"
        } else {
            "table"
        };
        entries.push(CatalogEntry {
            schema: Some(t.schema.clone()),
            name: t.name.clone(),
            entry_type: entry_type.to_string(),
            columns,
            row_count,
        });
    }

    Ok(entries)
}

fn snowflake_info_schema_entry_type(table_type: &str) -> &'static str {
    let u = table_type.trim().to_ascii_uppercase();
    if u.is_empty() {
        return "table";
    }
    match u.as_str() {
        "VIEW" | "MATERIALIZED VIEW" => "view",
        _ if u == "BASE TABLE"
            || u == "TABLE"
            || u == "TEMPORARY TABLE"
            || u == "TRANSIENT TABLE"
            || u == "EXTERNAL TABLE"
            || u == "EVENT TABLE"
            || u == "DYNAMIC TABLE"
            || u == "ICEBERG TABLE" =>
        {
            "table"
        }
        _ if u.ends_with(" VIEW") && !u.contains("TABLE") => "view",
        _ if u.contains("VIEW") && !u.contains("TABLE") => "view",
        _ => "table",
    }
}

fn snowflake_object_key(schema: &str, name: &str) -> (String, String) {
    (
        schema.trim().to_ascii_uppercase(),
        name.trim().to_ascii_uppercase(),
    )
}

fn quote_ident_segment(s: &str) -> String {
    s.replace('"', "\"\"")
}

/// Merge objects exposed by DuckDB's catalog functions (some Snowflake + DuckDB builds
/// omit or under-report rows in `INFORMATION_SCHEMA.TABLES` while still listing relations
/// in `duckdb_tables()` / `duckdb_views()`).
fn supplement_snowflake_from_duckdb_catalog(
    conn: &Connection,
    alias: &str,
    tables: &mut Vec<SnowflakeTableRow>,
) -> Result<(), AppError> {
    let safe = alias.replace('"', "");
    let esc = escape_sql_string(&safe);
    let mut known: std::collections::HashSet<(String, String)> = tables
        .iter()
        .map(|t| snowflake_object_key(&t.schema, &t.name))
        .collect();

    let sql = format!(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = '{}'",
        esc
    );
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(rows) = stmt.query_map(params![], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }) {
            for r in rows.flatten() {
                let k = snowflake_object_key(&r.0, &r.1);
                if known.insert(k) {
                    tables.push(SnowflakeTableRow {
                        schema: r.0,
                        name: r.1,
                        table_type: "BASE TABLE".to_string(),
                    });
                }
            }
        }
    }

    let sql_v = format!(
        "SELECT schema_name, view_name FROM duckdb_views() WHERE database_name = '{}' AND NOT internal",
        esc
    );
    if let Ok(mut stmt) = conn.prepare(&sql_v) {
        if let Ok(rows) = stmt.query_map(params![], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }) {
            for r in rows.flatten() {
                let k = snowflake_object_key(&r.0, &r.1);
                if known.insert(k) {
                    tables.push(SnowflakeTableRow {
                        schema: r.0,
                        name: r.1,
                        table_type: "VIEW".to_string(),
                    });
                }
            }
        }
    }

    Ok(())
}

#[derive(Clone)]
struct SnowflakeTableRow {
    schema: String,
    name: String,
    table_type: String,
}

/// Snowflake-optimised introspection: fetches all table and column metadata in
/// just 2 bulk queries over the network instead of per-table DESCRIBE + COUNT.
///
/// Queries must use the **attached catalog** (`"{alias}"."INFORMATION_SCHEMA"`),
/// not bare `information_schema` (that is DuckDB's own catalog and returns no
/// Snowflake tables).
///
/// `table_catalog` varies by driver: DuckDB's Snowflake attach often reports the
/// **attach alias**; some paths use the Snowflake **database** name. We filter with
/// OR so both match (avoids empty catalogs when only one form appears).
fn introspect_attached_snowflake(
    conn: &Connection,
    alias: &str,
    snowflake_database: &str,
) -> Result<Vec<CatalogEntry>, AppError> {
    let safe_alias = alias.replace('"', "");
    let db_trim = snowflake_database.trim();
    let catalog_predicate = if db_trim.is_empty() {
        format!(
            "UPPER(TRIM(table_catalog)) = UPPER('{}')",
            escape_sql_string(&safe_alias)
        )
    } else {
        format!(
            "(UPPER(TRIM(table_catalog)) = UPPER('{}') OR UPPER(TRIM(table_catalog)) = UPPER('{}'))",
            escape_sql_string(&safe_alias),
            escape_sql_string(db_trim)
        )
    };

    let tables_sql = format!(
        r#"SELECT table_schema, table_name, table_type
         FROM "{}"."INFORMATION_SCHEMA"."TABLES"
         WHERE {}
           AND table_schema NOT IN ('INFORMATION_SCHEMA')"#,
        safe_alias, catalog_predicate
    );
    let mut stmt = conn.prepare(&tables_sql)?;

    let mut tables: Vec<SnowflakeTableRow> = stmt
        .query_map(params![], |row| {
            Ok(SnowflakeTableRow {
                schema: row.get(0)?,
                name: row.get(1)?,
                table_type: row.get(2)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    let mut use_catalog_fallback = false;
    if tables.is_empty() {
        let fallback_tables = format!(
            r#"SELECT table_schema, table_name, table_type
         FROM "{}"."INFORMATION_SCHEMA"."TABLES"
         WHERE table_schema NOT IN ('INFORMATION_SCHEMA')"#,
            safe_alias
        );
        if let Ok(mut fb) = conn.prepare(&fallback_tables) {
            if let Ok(mapped) = fb.query_map(params![], |row| {
                Ok(SnowflakeTableRow {
                    schema: row.get(0)?,
                    name: row.get(1)?,
                    table_type: row.get(2)?,
                })
            }) {
                tables = mapped.filter_map(|r| r.ok()).collect();
                use_catalog_fallback = !tables.is_empty();
            }
        }
    }

    supplement_snowflake_from_duckdb_catalog(conn, alias, &mut tables)?;

    let mut seen_obj: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    tables.retain(|t| seen_obj.insert(snowflake_object_key(&t.schema, &t.name)));

    let columns_sql = if use_catalog_fallback {
        format!(
            r#"SELECT table_schema, table_name, column_name, data_type, is_nullable
         FROM "{}"."INFORMATION_SCHEMA"."COLUMNS"
         WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
         ORDER BY table_schema, table_name, ordinal_position"#,
            safe_alias
        )
    } else {
        format!(
            r#"SELECT table_schema, table_name, column_name, data_type, is_nullable
         FROM "{}"."INFORMATION_SCHEMA"."COLUMNS"
         WHERE {}
           AND table_schema NOT IN ('INFORMATION_SCHEMA')
         ORDER BY table_schema, table_name, ordinal_position"#,
            safe_alias, catalog_predicate
        )
    };

    let mut columns_map: std::collections::HashMap<(String, String), Vec<ColumnInfo>> =
        std::collections::HashMap::new();
    if let Ok(mut col_stmt) = conn.prepare(&columns_sql) {
        if let Ok(rows) = col_stmt.query_map(params![], |row| {
            let schema: String = row.get(0)?;
            let table: String = row.get(1)?;
            let col_name: String = row.get(2)?;
            let data_type: String = row.get(3)?;
            let nullable_str: String = row.get(4)?;
            Ok((schema, table, col_name, data_type, nullable_str))
        }) {
            for r in rows.flatten() {
                let key = snowflake_object_key(&r.0, &r.1);
                columns_map.entry(key).or_default().push(ColumnInfo {
                    name: r.2,
                    data_type: r.3,
                    nullable: r.4.eq_ignore_ascii_case("yes"),
                    key: None,
                });
            }
        }
    }

    let mut entries = Vec::with_capacity(tables.len());
    for t in &tables {
        let key = snowflake_object_key(&t.schema, &t.name);
        let mut columns = columns_map.remove(&key).unwrap_or_default();

        if columns.is_empty() {
            let sch_raw = t.schema.trim();
            let sch = if sch_raw.is_empty() {
                "PUBLIC"
            } else {
                sch_raw
            };
            let qualified = format!(
                "\"{}\".\"{}\".\"{}\"",
                safe_alias,
                quote_ident_segment(sch),
                quote_ident_segment(t.name.trim())
            );
            if let Ok(cols) = describe_ref(conn, &qualified) {
                columns = cols;
            }
        }

        let schema_out = {
            let s = t.schema.trim();
            if s.is_empty() {
                "PUBLIC".to_string()
            } else {
                s.to_string()
            }
        };

        let entry_type = snowflake_info_schema_entry_type(&t.table_type).to_string();
        entries.push(CatalogEntry {
            schema: Some(schema_out),
            name: t.name.trim().to_string(),
            entry_type,
            columns,
            row_count: None,
        });
    }

    Ok(entries)
}

fn introspect_catalog_for_server_alias(
    ct: ConnectorType,
    conn: &Connection,
    alias: &str,
    snowflake_database: Option<&str>,
) -> Result<Vec<CatalogEntry>, AppError> {
    match ct {
        ConnectorType::PostgreSQL => introspect_attached_postgres(conn, alias),
        ConnectorType::Snowflake => introspect_attached_snowflake(
            conn,
            alias,
            snowflake_database.unwrap_or(""),
        ),
        _ => Err(AppError::ConnectorError(
            "Not a PostgreSQL or Snowflake connector".into(),
        )),
    }
}

fn introspect_attached_db(
    conn: &Connection,
    alias: &str,
) -> Result<Vec<CatalogEntry>, AppError> {
    let tables_sql = format!(
        "SELECT table_schema, table_name, table_type \
         FROM information_schema.tables \
         WHERE table_catalog = '{}'",
        escape_sql_string(alias)
    );
    let mut stmt = conn.prepare(&tables_sql)?;

    struct TableInfo {
        schema: String,
        name: String,
        table_type: String,
    }

    let tables: Vec<TableInfo> = stmt
        .query_map(params![], |row| {
            Ok(TableInfo {
                schema: row.get(0)?,
                name: row.get(1)?,
                table_type: row.get(2)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    let mut entries = Vec::with_capacity(tables.len());
    for t in &tables {
        let qualified = format!("\"{}\".\"{}\".\"{}\"", alias, t.schema, t.name);
        let columns = describe_ref(conn, &qualified).unwrap_or_default();
        let row_count = count_ref(conn, &qualified);
        let entry_type = if t.table_type.contains("VIEW") {
            "view"
        } else {
            "table"
        };
        entries.push(CatalogEntry {
            schema: Some(t.schema.clone()),
            name: t.name.clone(),
            entry_type: entry_type.to_string(),
            columns,
            row_count,
        });
    }

    Ok(entries)
}

// ──────────────────────────── Tauri Commands ────────────────────────────

#[tauri::command]
pub fn add_connector(
    name: String,
    connector_type: ConnectorType,
    config: ConnectorConfig,
    access_key: Option<String>,
    secret_key: Option<String>,
    state: State<'_, AppState>,
) -> Result<Connector, AppError> {
    let id = uuid::Uuid::new_v4().to_string();

    let needs_alias = matches!(
        connector_type,
        ConnectorType::SQLite
            | ConnectorType::DuckDB
            | ConnectorType::PostgreSQL
            | ConnectorType::Snowflake
            | ConnectorType::DuckLake
    );
    let alias = if needs_alias {
        Some(sanitize_alias(&name))
    } else {
        None
    };

    let is_cloud = matches!(
        connector_type,
        ConnectorType::S3 | ConnectorType::GCS | ConnectorType::R2
    );
    let needs_secret =
        is_cloud || matches!(connector_type, ConnectorType::Snowflake);
    let secret_name = if needs_secret {
        Some(format!("cdv_{}", id.replace('-', "_")))
    } else {
        None
    };

    let mut final_config = config;
    if is_cloud {
        if let Some(ak) = &access_key {
            final_config.user = Some(ak.clone());
        }
        if let Some(sk) = &secret_key {
            final_config.password = Some(sk.clone());
        }
    } else if matches!(
        connector_type,
        ConnectorType::PostgreSQL | ConnectorType::Snowflake
    ) {
        if let Some(sk) = &secret_key {
            final_config.password = Some(sk.clone());
        }
    }

    let connector = Connector {
        id: id.clone(),
        name,
        connector_type,
        config: final_config,
        alias,
        secret_name,
    };

    let ops = get_ops(&connector.connector_type);
    let conn = state.conn.lock();
    ops.activate(&conn, &connector)?;
    drop(conn);

    state
        .connectors
        .lock()
        .insert(id.clone(), connector.clone());

    catalog::save_state_catalog(&state);

    Ok(connector)
}

#[tauri::command]
pub fn remove_connector(id: String, state: State<'_, AppState>) -> Result<(), AppError> {
    let connector = {
        let mut connectors = state.connectors.lock();
        connectors
            .remove(&id)
            .ok_or_else(|| AppError::ConnectorError("Connector not found".into()))?
    };

    let secondaries = state
        .connector_secondary_attaches
        .lock()
        .remove(&id)
        .unwrap_or_default();

    let ops = get_ops(&connector.connector_type);
    let conn = state.conn.lock();
    detach_secondary_attaches(&conn, &secondaries);
    ops.deactivate(&conn, &connector).ok();
    drop(conn);

    {
        let mut sources = state.data_sources.lock();
        sources.retain(|_, ds| ds.connector_id != id);
    }

    state.connector_catalogs_by_db.lock().remove(&id);
    state.connector_database_names.lock().remove(&id);
    catalog::save_state_catalog(&state);

    Ok(())
}

#[tauri::command]
pub fn test_connector(
    connector_type: ConnectorType,
    config: ConnectorConfig,
    access_key: Option<String>,
    secret_key: Option<String>,
    state: State<'_, AppState>,
) -> Result<(), AppError> {
    let mut final_config = config;
    let is_cloud = matches!(
        connector_type,
        ConnectorType::S3 | ConnectorType::GCS | ConnectorType::R2
    );
    if is_cloud {
        if let Some(ak) = &access_key {
            final_config.user = Some(ak.clone());
        }
        if let Some(sk) = &secret_key {
            final_config.password = Some(sk.clone());
        }
    } else if matches!(
        connector_type,
        ConnectorType::PostgreSQL | ConnectorType::Snowflake
    ) {
        if let Some(sk) = &secret_key {
            final_config.password = Some(sk.clone());
        }
    }

    let needs_alias = matches!(
        connector_type,
        ConnectorType::SQLite
            | ConnectorType::DuckDB
            | ConnectorType::PostgreSQL
            | ConnectorType::Snowflake
            | ConnectorType::DuckLake
    );
    let alias = if needs_alias {
        Some("__cdv_test__".to_string())
    } else {
        None
    };

    let needs_secret =
        is_cloud || matches!(connector_type, ConnectorType::Snowflake);
    let secret_name = if needs_secret {
        Some("__cdv_test_secret__".to_string())
    } else {
        None
    };

    let temp = Connector {
        id: String::new(),
        name: String::new(),
        connector_type,
        config: final_config,
        alias,
        secret_name,
    };

    let ops = get_ops(&temp.connector_type);
    let conn = state.meta_conn.lock();
    let result = ops.test(&conn, &temp);
    ops.deactivate(&conn, &temp).ok();
    result
}

#[tauri::command]
pub fn introspect_connector(
    id: String,
    state: State<'_, AppState>,
) -> Result<Vec<CatalogEntry>, AppError> {
    let connector = {
        let connectors = state.connectors.lock();
        connectors
            .get(&id)
            .cloned()
            .ok_or_else(|| AppError::ConnectorError("Connector not found".into()))?
    };

    let db_key = catalog_db_key_for_connector(&connector);
    let ops = get_ops(&connector.connector_type);
    let conn = state.conn.lock();
    let entries = if matches!(
        connector.connector_type,
        ConnectorType::PostgreSQL | ConnectorType::Snowflake
    ) {
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Database connector missing alias".into()))?;
        let sf_filter: Option<String> =
            if matches!(connector.connector_type, ConnectorType::Snowflake) {
                connector.config.database.as_ref().map(|d| normalize_snowflake_database_name(d))
            } else {
                None
            };
        let snowflake_db = sf_filter
            .as_deref()
            .filter(|s| *s != SINGLE_DB_CATALOG_KEY);
        introspect_catalog_for_server_alias(
            connector.connector_type,
            &conn,
            alias,
            snowflake_db,
        )?
    } else {
        ops.introspect(&conn, &connector)?
    };
    drop(conn);

    state
        .connector_catalogs_by_db
        .lock()
        .entry(id.clone())
        .or_default()
        .insert(db_key, entries.clone());
    catalog::save_state_catalog(&state);

    Ok(entries)
}

fn list_pg_databases_with_config(
    conn: &Connection,
    cfg: &ConnectorConfig,
) -> Result<Vec<String>, AppError> {
    ensure_extension(conn, "postgres")?;

    let mut c = cfg.clone();
    c.database = Some("postgres".to_string());
    let connstr = build_pg_connstr(&c)?;
    let alias = "__cdv_list_dbs__";

    conn.execute_batch(&format!(
        "ATTACH '{}' AS \"{}\" (TYPE POSTGRES, READ_ONLY)",
        escape_sql_string(&connstr),
        alias
    ))
    .map_err(|e| AppError::ConnectorError(format!("Failed to connect to PostgreSQL: {}", e)))?;

    let sql = format!(
        "SELECT datname FROM \"{}\".pg_catalog.pg_database \
         WHERE datistemplate = false ORDER BY datname",
        alias
    );
    let mut stmt = conn.prepare(&sql)?;
    let dbs: Vec<String> = stmt
        .query_map(params![], |row| row.get(0))
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default();

    conn.execute_batch(&format!("DETACH \"{}\"", alias)).ok();
    Ok(dbs)
}

/// Full Snowflake database list (can be slow). Only call with `refresh: true` / explicit refresh.
fn list_snowflake_databases_deep(conn: &Connection, alias: &str) -> Result<Vec<String>, AppError> {
    let safe = alias.replace('"', "");
    let sql = format!(
        r#"SELECT DISTINCT catalog_name FROM "{}"."INFORMATION_SCHEMA"."SCHEMATA" ORDER BY 1"#,
        safe
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| AppError::ConnectorError(format!("Snowflake list databases failed: {}", e)))?;
    let dbs: Vec<String> = stmt
        .query_map(params![], |row| row.get(0))
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default();
    Ok(dbs)
}

/// List databases for a connector. Cached in `connector_database_names` and persisted.
/// - `refresh == false` (default): return **cached** list if non-empty; otherwise a **cheap** result
///   (PostgreSQL: query server; Snowflake: configured default DB only so the UI can render immediately).
/// - `refresh == true`: re-query the server (Snowflake runs the full INFORMATION_SCHEMA scan).
#[tauri::command]
pub fn list_connector_databases(
    id: String,
    refresh: Option<bool>,
    state: State<'_, AppState>,
) -> Result<Vec<String>, AppError> {
    let connector = {
        let connectors = state.connectors.lock();
        connectors
            .get(&id)
            .cloned()
            .ok_or_else(|| AppError::ConnectorError("Connector not found".into()))?
    };

    let refresh = refresh.unwrap_or(false);

    let dbs = match connector.connector_type {
        ConnectorType::PostgreSQL => {
            if !refresh {
                let cached = state
                    .connector_database_names
                    .lock()
                    .get(&id)
                    .cloned()
                    .filter(|v| !v.is_empty());
                if let Some(names) = cached {
                    return Ok(names);
                }
            }
            let conn = state.conn.lock();
            list_pg_databases_with_config(&conn, &connector.config)?
        }
        ConnectorType::Snowflake => {
            if !refresh {
                let cached = state
                    .connector_database_names
                    .lock()
                    .get(&id)
                    .cloned()
                    .filter(|v| !v.is_empty());
                if let Some(names) = cached {
                    return Ok(names);
                }
                let minimal: Vec<String> = connector
                    .config
                    .database
                    .clone()
                    .into_iter()
                    .filter(|s| !s.is_empty())
                    .map(|s| normalize_snowflake_database_name(&s))
                    .collect();
                state
                    .connector_database_names
                    .lock()
                    .insert(id.clone(), minimal.clone());
                catalog::save_state_catalog(&state);
                return Ok(minimal);
            }
            let conn = state.conn.lock();
            let alias = connector
                .alias
                .as_deref()
                .ok_or_else(|| AppError::ConnectorError("Snowflake connector missing alias".into()))?;
            let mut v: Vec<String> = list_snowflake_databases_deep(&conn, alias)?
                .into_iter()
                .map(|s| normalize_snowflake_database_name(&s))
                .collect();
            if let Some(db) = connector.config.database.clone().filter(|s| !s.is_empty()) {
                let nd = normalize_snowflake_database_name(&db);
                if !v.iter().any(|x| x == &nd) {
                    v.push(nd);
                }
            }
            v.sort();
            v.dedup();
            if v.is_empty() {
                if let Some(db) = connector.config.database.clone().filter(|s| !s.is_empty()) {
                    v.push(normalize_snowflake_database_name(&db));
                }
            }
            v
        }
        _ => {
            return Err(AppError::ConnectorError(
                "Connector does not support database listing".into(),
            ));
        }
    };

    state
        .connector_database_names
        .lock()
        .insert(id.clone(), dbs.clone());
    catalog::save_state_catalog(&state);

    Ok(dbs)
}

#[tauri::command]
pub fn connect_connector_database(
    id: String,
    database: String,
    state: State<'_, AppState>,
) -> Result<Vec<CatalogEntry>, AppError> {
    let connector = {
        let connectors = state.connectors.lock();
        connectors
            .get(&id)
            .cloned()
            .ok_or_else(|| AppError::ConnectorError("Connector not found".into()))?
    };

    if !matches!(
        connector.connector_type,
        ConnectorType::PostgreSQL | ConnectorType::Snowflake
    ) {
        return Err(AppError::ConnectorError(
            "Only PostgreSQL and Snowflake support per-database connection".into(),
        ));
    }

    let database = if matches!(connector.connector_type, ConnectorType::Snowflake) {
        normalize_snowflake_database_name(&database)
    } else {
        database
    };

    let default_key = catalog_db_key_for_connector(&connector);

    if database == default_key {
        let alias = connector
            .alias
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Database connector missing alias".into()))?;
        let conn = state.conn.lock();
        let snowflake_db = match connector.connector_type {
            ConnectorType::Snowflake => Some(database.as_str()),
            _ => None,
        };
        let entries = introspect_catalog_for_server_alias(
            connector.connector_type,
            &conn,
            alias,
            snowflake_db,
        )?;
        drop(conn);

        state
            .connector_catalogs_by_db
            .lock()
            .entry(id.clone())
            .or_default()
            .insert(default_key, entries.clone());
        catalog::save_state_catalog(&state);
        return Ok(entries);
    }

    let already = state
        .connector_secondary_attaches
        .lock()
        .get(&id)
        .and_then(|v| v.iter().find(|s| s.database == database).cloned());

    let attach_alias = if let Some(s) = already {
        s.attach_alias
    } else {
        let attach_alias = make_secondary_attach_alias(&connector, &database);
        let sf_secret = if matches!(connector.connector_type, ConnectorType::Snowflake) {
            Some(snowflake_secondary_secret_name(&connector, &database))
        } else {
            None
        };
        {
            let conn = state.conn.lock();
            match connector.connector_type {
                ConnectorType::PostgreSQL => {
                    attach_postgres_database(&conn, &connector, &database, &attach_alias)?;
                }
                ConnectorType::Snowflake => {
                    let secret_name = sf_secret.as_ref().ok_or_else(|| {
                        AppError::ConnectorError("Snowflake secondary secret name missing".into())
                    })?;
                    drop_secret_if_exists(&conn, secret_name);
                    let sql = build_snowflake_secret_sql(&connector, &database, secret_name)?;
                    conn.execute_batch(&sql).map_err(|e| {
                        AppError::ConnectorError(format!("Failed to create Snowflake secret: {}", e))
                    })?;
                    attach_snowflake_with_secret(&conn, &attach_alias, secret_name)?;
                }
                _ => unreachable!(),
            }
        }
        state
            .connector_secondary_attaches
            .lock()
            .entry(id.clone())
            .or_default()
            .push(SecondaryAttach {
                database: database.clone(),
                attach_alias: attach_alias.clone(),
                secret_name: sf_secret,
            });
        attach_alias
    };

    let conn = state.conn.lock();
    let snowflake_db = match connector.connector_type {
        ConnectorType::Snowflake => Some(database.as_str()),
        _ => None,
    };
    let entries = introspect_catalog_for_server_alias(
        connector.connector_type,
        &conn,
        &attach_alias,
        snowflake_db,
    )?;
    drop(conn);

    state
        .connector_catalogs_by_db
        .lock()
        .entry(id.clone())
        .or_default()
        .insert(database.clone(), entries.clone());
    catalog::save_state_catalog(&state);

    Ok(entries)
}

#[tauri::command]
pub fn list_pg_databases(
    config: ConnectorConfig,
    secret_key: Option<String>,
    state: State<'_, AppState>,
) -> Result<Vec<String>, AppError> {
    let conn = state.conn.lock();
    let mut cfg = config;
    if let Some(sk) = secret_key {
        cfg.password = Some(sk);
    }
    list_pg_databases_with_config(&conn, &cfg)
}

#[tauri::command]
pub fn get_cached_catalogs(
    state: State<'_, AppState>,
) -> Result<HashMap<String, ConnectorBrowseCache>, AppError> {
    let connectors = state.connectors.lock().clone();
    let by_db = state.connector_catalogs_by_db.lock().clone();
    let db_names = state.connector_database_names.lock().clone();
    let secondaries = state.connector_secondary_attaches.lock().clone();

    let mut out = HashMap::new();
    for (id, c) in connectors.iter() {
        let mut cache = ConnectorBrowseCache::default();
        let key = catalog_db_key_for_connector(c);
        cache.default_database = key.clone();
        if let Some(m) = by_db.get(id) {
            cache.catalogs_by_database = m.clone();
        }
        if is_multi_database_connector(c) {
            cache.database_names = db_names.get(id).cloned().unwrap_or_default();
        }
        if let Some(base) = c.alias.clone() {
            cache
                .attach_aliases_by_database
                .insert(key.clone(), base);
        }
        if let Some(sec) = secondaries.get(id) {
            for s in sec {
                cache
                    .attach_aliases_by_database
                    .insert(s.database.clone(), s.attach_alias.clone());
            }
        }
        out.insert(id.clone(), cache);
    }
    Ok(out)
}

#[tauri::command]
pub fn list_connectors(state: State<'_, AppState>) -> Result<Vec<Connector>, AppError> {
    Ok(state.connectors.lock().values().cloned().collect())
}
