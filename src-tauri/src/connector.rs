use std::path::Path;

use duckdb::params;
use duckdb::Connection;
use tauri::State;

use crate::catalog;
use crate::error::AppError;
use crate::state::{AppState, CatalogEntry, ColumnInfo, Connector, ConnectorConfig, ConnectorType};

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
        "CREATE SECRET \"{}\" ({})",
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
            conn.execute_batch(&format!("DROP SECRET IF EXISTS \"{}\"", secret_name))
                .map_err(|e| AppError::ConnectorError(e.to_string()))?;
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

// ──────────────────────────── SnowflakeOps ────────────────────────────

pub struct SnowflakeOps;

fn snowflake_secret_name(connector: &Connector) -> String {
    connector
        .secret_name
        .clone()
        .unwrap_or_else(|| format!("cdv_sf_{}", connector.alias.as_deref().unwrap_or("sf")))
}

fn extract_snowflake_account(host: &str) -> &str {
    host.strip_suffix(".snowflakecomputing.com")
        .unwrap_or(host)
}

impl ConnectorOps for SnowflakeOps {
    fn activate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
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

        let secret_sql = format!(
            "CREATE SECRET \"{}\" ({})",
            secret_name,
            secret_parts.join(", ")
        );
        conn.execute_batch(&secret_sql).map_err(|e| {
            AppError::ConnectorError(format!("Failed to create Snowflake secret: {}", e))
        })?;

        let attach_sql = format!(
            "ATTACH '' AS \"{}\" (TYPE snowflake, SECRET \"{}\", READ_ONLY)",
            alias, secret_name
        );
        conn.execute_batch(&attach_sql).map_err(|e| {
            AppError::ConnectorError(format!("Failed to attach Snowflake database: {}", e))
        })?;

        Ok(())
    }

    fn deactivate(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        if let Some(alias) = &connector.alias {
            conn.execute_batch(&format!("DETACH \"{}\"", alias))
                .map_err(|e| {
                    AppError::ConnectorError(format!("Failed to detach Snowflake: {}", e))
                })?;
        }
        let secret_name = snowflake_secret_name(connector);
        conn.execute_batch(&format!("DROP SECRET IF EXISTS \"{}\"", secret_name))
            .map_err(|e| AppError::ConnectorError(e.to_string()))?;
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
        introspect_attached_db(conn, alias)
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

    let ops = get_ops(&connector.connector_type);
    let conn = state.conn.lock();
    ops.deactivate(&conn, &connector).ok();
    drop(conn);

    {
        let mut sources = state.data_sources.lock();
        sources.retain(|_, ds| ds.connector_id != id);
    }

    state.connector_catalogs.lock().remove(&id);
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

    let ops = get_ops(&connector.connector_type);
    let conn = state.meta_conn.lock();
    let entries = ops.introspect(&conn, &connector)?;
    drop(conn);

    state
        .connector_catalogs
        .lock()
        .insert(id, entries.clone());
    catalog::save_state_catalog(&state);

    Ok(entries)
}

#[tauri::command]
pub fn list_pg_databases(
    config: ConnectorConfig,
    secret_key: Option<String>,
    state: State<'_, AppState>,
) -> Result<Vec<String>, AppError> {
    let conn = state.meta_conn.lock();
    ensure_extension(&conn, "postgres")?;

    let mut cfg = config;
    cfg.database = Some("postgres".to_string());
    if let Some(sk) = secret_key {
        cfg.password = Some(sk);
    }
    let connstr = build_pg_connstr(&cfg)?;
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

#[tauri::command]
pub fn get_cached_catalogs(
    state: State<'_, AppState>,
) -> Result<std::collections::HashMap<String, Vec<CatalogEntry>>, AppError> {
    Ok(state.connector_catalogs.lock().clone())
}

#[tauri::command]
pub fn list_connectors(state: State<'_, AppState>) -> Result<Vec<Connector>, AppError> {
    Ok(state.connectors.lock().values().cloned().collect())
}
