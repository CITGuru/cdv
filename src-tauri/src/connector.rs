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

fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn is_url(path: &str) -> bool {
    path.starts_with("http://") || path.starts_with("https://")
}

fn ensure_extension(conn: &Connection, ext: &str) -> Result<(), AppError> {
    conn.execute_batch(&format!("INSTALL '{}'; LOAD '{}';", ext, ext))
        .map_err(|e| {
            AppError::ConnectorError(format!("Failed to load {} extension: {}", ext, e))
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
        introspect_attached_db(conn, alias)
    }

    fn test(&self, conn: &Connection, connector: &Connector) -> Result<(), AppError> {
        self.activate(conn, connector)?;
        self.deactivate(conn, connector).ok();
        Ok(())
    }
}

// ──────────────────────────── SnowflakeOps (stub) ────────────────────────────

pub struct SnowflakeOps;

impl ConnectorOps for SnowflakeOps {
    fn activate(&self, _conn: &Connection, _connector: &Connector) -> Result<(), AppError> {
        Err(AppError::ConnectorError(
            "Snowflake support coming soon".into(),
        ))
    }
    fn deactivate(&self, _conn: &Connection, _connector: &Connector) -> Result<(), AppError> {
        Ok(())
    }
    fn introspect(
        &self,
        _conn: &Connection,
        _connector: &Connector,
    ) -> Result<Vec<CatalogEntry>, AppError> {
        Err(AppError::ConnectorError(
            "Snowflake support coming soon".into(),
        ))
    }
    fn test(&self, _conn: &Connection, _connector: &Connector) -> Result<(), AppError> {
        Err(AppError::ConnectorError(
            "Snowflake support coming soon".into(),
        ))
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
    let secret_name = if is_cloud {
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
    );
    let alias = if needs_alias {
        Some("__cdv_test__".to_string())
    } else {
        None
    };

    let secret_name = if is_cloud {
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
    let conn = state.conn.lock();
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
    let conn = state.conn.lock();
    ops.introspect(&conn, &connector)
}

#[tauri::command]
pub fn list_connectors(state: State<'_, AppState>) -> Result<Vec<Connector>, AppError> {
    Ok(state.connectors.lock().values().cloned().collect())
}
