use std::path::Path;

use duckdb::arrow::record_batch::RecordBatch;
use duckdb::params;
use duckdb::Connection;
use tauri::{Manager, State};

use crate::catalog;
use crate::connector::resolve_attach_alias_for_database;
use crate::error::AppError;
use crate::state::{
    catalog_db_key_for_connector, normalize_snowflake_database_name, AppState, ColumnInfo,
    ConnectorType, DataSource, Driver,
    FilePreview,
};

pub fn build_duckdb_ref(path: &str, format: &str) -> Result<String, AppError> {
    let (_, duckdb_ref) = format_to_ref(path, format)?;
    Ok(duckdb_ref)
}

fn detect_format(path: &str) -> Result<(String, String), AppError> {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    format_to_ref(path, &ext)
}

fn escape_path_for_sql(path: &str) -> String {
    path.replace('\\', "\\\\").replace('\'', "''")
}

pub(crate) fn format_to_ref(path: &str, format: &str) -> Result<(String, String), AppError> {
    let path_esc = escape_path_for_sql(path);
    match format {
        "csv" => Ok(("csv".to_string(), format!("read_csv_auto('{}')", path_esc))),
        "tsv" => Ok((
            "tsv".to_string(),
            format!("read_csv_auto('{}', delim='\\t')", path_esc),
        )),
        "json" => Ok((
            "json".to_string(),
            format!("read_json_auto('{}')", path_esc),
        )),
        "jsonl" => Ok((
            "jsonl".to_string(),
            format!("read_json_auto('{}')", path_esc),
        )),
        "parquet" => Ok((
            "parquet".to_string(),
            format!("read_parquet('{}')", path_esc),
        )),
        "xlsx" => Ok(("xlsx".to_string(), format!("read_xlsx('{}')", path_esc))),
        "avro" => Ok(("avro".to_string(), format!("read_avro('{}')", path_esc))),
        "arrow" | "ipc" | "arrow_ipc" => {
            Ok(("arrow_ipc".to_string(), format!("'{}'", path_esc)))
        }
        _ => Err(AppError::FileError(format!(
            "Unsupported file format: {}",
            format
        ))),
    }
}

pub(crate) fn describe_ref(
    conn: &Connection,
    duckdb_ref: &str,
) -> Result<Vec<ColumnInfo>, AppError> {
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

pub(crate) fn count_ref(conn: &Connection, duckdb_ref: &str) -> Option<u64> {
    let sql = format!("SELECT COUNT(*) FROM {}", duckdb_ref);
    conn.query_row(&sql, params![], |row| row.get(0)).ok()
}

pub fn rehydrate_views(
    conn: &Connection,
    data_sources: &[DataSource],
    connectors: &std::collections::HashMap<String, crate::state::Connector>,
) -> Result<(), AppError> {
    for ds in data_sources {
        if ds.kind == "table" {
            continue;
        }
        if ds.kind == "external" {
            continue;
        }
        let view_name = match &ds.view_name {
            Some(v) => v,
            None => continue,
        };
        let connector = match connectors.get(&ds.connector_id) {
            Some(c) => c,
            None => continue,
        };
        if !matches!(connector.connector_type, ConnectorType::LocalFile) {
            continue;
        }
        let path = match &connector.config.path {
            Some(p) => p,
            None => continue,
        };
        let format = match &connector.config.format {
            Some(f) => f,
            None => continue,
        };
        let duckdb_ref = build_duckdb_ref(path, format)?;
        let create_sql = format!(
            "CREATE OR REPLACE VIEW \"{}\" AS SELECT * FROM {}",
            view_name, duckdb_ref
        );
        conn.execute_batch(&create_sql)?;
    }
    Ok(())
}

fn sanitize_view_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = sanitized.trim_matches('_').to_lowercase();
    if trimmed.is_empty() {
        "untitled_view".to_string()
    } else if trimmed
        .chars()
        .next()
        .map_or(true, |c| c.is_numeric())
    {
        format!("v_{}", trimmed)
    } else {
        trimmed
    }
}

#[tauri::command]
pub fn preview_file(
    path: String,
    format: Option<String>,
    state: State<'_, AppState>,
) -> Result<FilePreview, AppError> {
    let (fmt, duckdb_ref) = match &format {
        Some(f) => format_to_ref(&path, f)?,
        None => detect_format(&path)?,
    };

    let conn = state.meta_conn.lock();
    let schema = describe_ref(&conn, &duckdb_ref)?;
    let row_count = count_ref(&conn, &duckdb_ref);

    let preview_sql = format!("SELECT * FROM {} LIMIT 50", duckdb_ref);
    let mut stmt = conn.prepare(&preview_sql)?;
    let frames = stmt.query_arrow(params![])?;
    let batches: Vec<RecordBatch> = frames.collect();

    let preview_data = if batches.is_empty() {
        Vec::new()
    } else {
        crate::query_engine::batches_to_ipc(&batches[0].schema(), &batches)?
    };

    Ok(FilePreview {
        format: fmt,
        schema,
        row_count,
        preview_data,
    })
}

#[tauri::command]
pub fn create_data_source(
    name: String,
    view_name: String,
    connector_id: String,
    materialize: Option<bool>,
    primary_key_column: Option<String>,
    db_schema: Option<String>,
    db_table: Option<String>,
    db_database: Option<String>,
    driver: Option<Driver>,
    state: State<'_, AppState>,
) -> Result<DataSource, AppError> {
    let connector = {
        let connectors = state.connectors.lock();
        connectors
            .get(&connector_id)
            .cloned()
            .ok_or_else(|| AppError::FileError("Connector not found".to_string()))?
    };

    let conn = state.conn.lock();

    let is_db_connector = matches!(
        connector.connector_type,
        ConnectorType::SQLite
            | ConnectorType::DuckDB
            | ConnectorType::PostgreSQL
            | ConnectorType::Snowflake
            | ConnectorType::DuckLake
    );

    if is_db_connector {
        let secondaries = state
            .connector_secondary_attaches
            .lock()
            .get(&connector_id)
            .cloned()
            .unwrap_or_default();
        let alias = resolve_attach_alias_for_database(
            &connector,
            db_database.as_deref().filter(|s| !s.is_empty()),
            &secondaries,
        )?;
        let schema_name = db_schema
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Missing schema for database table".into()))?;
        let table_name = db_table
            .as_deref()
            .ok_or_else(|| AppError::ConnectorError("Missing table name for database table".into()))?;

        let qualified_name = format!(
            "\"{}\".\"{}\".\"{}\"",
            alias, schema_name, table_name
        );

        let default_db_key = catalog_db_key_for_connector(&connector);
        let lookup_db_key: String = {
            let raw = db_database
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or(default_db_key.as_str());
            if matches!(connector.connector_type, ConnectorType::Snowflake) {
                normalize_snowflake_database_name(raw)
            } else {
                raw.to_string()
            }
        };

        let cached_entry = state
            .connector_catalogs_by_db
            .lock()
            .get(&connector_id)
            .and_then(|m| m.get(&lookup_db_key))
            .and_then(|entries| {
                entries.iter().find(|e| {
                    e.name == table_name && e.schema.as_deref() == Some(schema_name)
                }).cloned()
            });

        let (columns, row_count) = if let Some(entry) = cached_entry {
            (entry.columns, entry.row_count)
        } else if matches!(connector.connector_type, ConnectorType::PostgreSQL) {
            let columns = describe_ref(&conn, &qualified_name).unwrap_or_default();
            let approx_sql = format!(
                "SELECT c.reltuples::BIGINT \
                 FROM \"{alias}\".pg_catalog.pg_class c \
                 JOIN \"{alias}\".pg_catalog.pg_namespace n ON c.relnamespace = n.oid \
                 WHERE n.nspname = '{}' AND c.relname = '{}'",
                schema_name.replace('\'', "''"),
                table_name.replace('\'', "''")
            );
            let row_count = conn
                .query_row(&approx_sql, params![], |row| row.get::<_, i64>(0))
                .ok()
                .and_then(|c| if c < 0 { None } else { Some(c as u64) });
            (columns, row_count)
        } else {
            let columns = describe_ref(&conn, &qualified_name).unwrap_or_default();
            let row_count = count_ref(&conn, &qualified_name);
            (columns, row_count)
        };

        let resolved_driver = driver.unwrap_or_else(|| connector.connector_type.default_driver());
        let id = uuid::Uuid::new_v4().to_string();
        let data_source = DataSource {
            id: id.clone(),
            name,
            connector_id,
            qualified_name,
            view_name: None,
            schema: columns,
            row_count,
            kind: "external".to_string(),
            primary_key_column: primary_key_column.filter(|s| !s.is_empty()),
            driver: resolved_driver,
        };

        drop(conn);
        state.data_sources.lock().insert(id, data_source.clone());
        catalog::save_state_catalog(&state);

        return Ok(data_source);
    }

    // File-based connector
    let path = connector
        .config
        .path
        .as_deref()
        .ok_or_else(|| AppError::FileError("File connector missing path".into()))?;
    let format = connector
        .config
        .format
        .as_deref()
        .ok_or_else(|| AppError::FileError("File connector missing format".into()))?;

    let safe_view = sanitize_view_name(&view_name);
    let materialize = materialize.unwrap_or(false);
    let kind = if materialize { "table" } else { "view" };

    let (_, duckdb_ref) = format_to_ref(path, format)?;

    if materialize {
        let create_sql = format!(
            "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM {}",
            safe_view, duckdb_ref
        );
        conn.execute_batch(&create_sql)?;
    } else {
        let create_sql = format!(
            "CREATE OR REPLACE VIEW \"{}\" AS SELECT * FROM {}",
            safe_view, duckdb_ref
        );
        conn.execute_batch(&create_sql)?;
    }

    let quoted = format!("\"{}\"", safe_view);
    let schema = describe_ref(&conn, &quoted)?;
    let row_count = count_ref(&conn, &quoted);

    let resolved_driver = driver.unwrap_or_else(|| connector.connector_type.default_driver());
    let id = uuid::Uuid::new_v4().to_string();
    let data_source = DataSource {
        id: id.clone(),
        name,
        connector_id,
        qualified_name: quoted,
        view_name: Some(safe_view),
        schema,
        row_count,
        kind: kind.to_string(),
        primary_key_column: primary_key_column.filter(|s| !s.is_empty()),
        driver: resolved_driver,
    };

    drop(conn);
    state.data_sources.lock().insert(id, data_source.clone());
    catalog::save_state_catalog(&state);

    Ok(data_source)
}

#[tauri::command]
pub fn remove_data_source(id: String, state: State<'_, AppState>) -> Result<(), AppError> {
    let ds = {
        let mut sources = state.data_sources.lock();
        sources
            .remove(&id)
            .ok_or_else(|| AppError::FileError("Data source not found".to_string()))?
    };

    if let Some(view_name) = &ds.view_name {
        let conn = state.conn.lock();
        if ds.kind == "table" {
            conn.execute_batch(&format!("DROP TABLE IF EXISTS \"{}\"", view_name))?;
        } else if ds.kind == "view" {
            conn.execute_batch(&format!("DROP VIEW IF EXISTS \"{}\"", view_name))?;
        }
    }

    catalog::save_state_catalog(&state);

    Ok(())
}

#[tauri::command]
pub fn list_data_sources(state: State<'_, AppState>) -> Result<Vec<DataSource>, AppError> {
    Ok(state.data_sources.lock().values().cloned().collect())
}

#[tauri::command]
pub fn get_schema(
    dataset_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<ColumnInfo>, AppError> {
    let sources = state.data_sources.lock();
    let ds = sources
        .get(&dataset_id)
        .ok_or_else(|| AppError::FileError(format!("Data source not found: {}", dataset_id)))?;
    Ok(ds.schema.clone())
}

#[tauri::command]
pub fn get_preview(dataset_id: String, state: State<'_, AppState>) -> Result<Vec<u8>, AppError> {
    let qualified_name = {
        let sources = state.data_sources.lock();
        let ds = sources.get(&dataset_id).ok_or_else(|| {
            AppError::FileError(format!("Data source not found: {}", dataset_id))
        })?;
        ds.qualified_name.clone()
    };

    let conn = state.conn.lock();
    let sql = format!("SELECT * FROM {} LIMIT 100", qualified_name);
    let mut stmt = conn.prepare(&sql)?;
    let frames = stmt.query_arrow(params![])?;
    let batches: Vec<RecordBatch> = frames.collect();

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    crate::query_engine::batches_to_ipc(&batches[0].schema(), &batches)
}

#[tauri::command]
pub fn update_data_source(
    id: String,
    name: Option<String>,
    view_name: Option<String>,
    state: State<'_, AppState>,
) -> Result<DataSource, AppError> {
    let mut sources = state.data_sources.lock();
    let ds = sources
        .get_mut(&id)
        .ok_or_else(|| AppError::FileError("Data source not found".to_string()))?;

    if ds.kind == "external" {
        if let Some(n) = name {
            ds.name = n;
        }
        let result = ds.clone();
        drop(sources);
        catalog::save_state_catalog(&state);
        return Ok(result);
    }

    let old_view_name = ds.view_name.clone().unwrap_or_default();
    let new_view_name = view_name
        .as_ref()
        .map(|v| sanitize_view_name(v))
        .unwrap_or_else(|| sanitize_view_name(&old_view_name));

    if new_view_name != old_view_name && !old_view_name.is_empty() {
        let conn = state.conn.lock();
        if ds.kind == "table" {
            conn.execute_batch(&format!(
                "ALTER TABLE \"{}\" RENAME TO \"{}\"",
                old_view_name, new_view_name
            ))?;
        } else {
            let connector = {
                let connectors = state.connectors.lock();
                connectors.get(&ds.connector_id).cloned()
            };
            if let Some(c) = connector {
                if let (Some(path), Some(format)) = (&c.config.path, &c.config.format) {
                    let duckdb_ref = build_duckdb_ref(path, format)?;
                    conn.execute_batch(&format!(
                        "DROP VIEW IF EXISTS \"{}\"",
                        old_view_name
                    ))?;
                    conn.execute_batch(&format!(
                        "CREATE OR REPLACE VIEW \"{}\" AS SELECT * FROM {}",
                        new_view_name, duckdb_ref
                    ))?;
                }
            }
        }
        ds.view_name = Some(new_view_name.clone());
        ds.qualified_name = format!("\"{}\"", new_view_name);
    }

    if let Some(n) = name {
        ds.name = n;
    }
    let result = ds.clone();
    drop(sources);
    catalog::save_state_catalog(&state);

    Ok(result)
}

#[tauri::command]
pub fn refresh_data_source(id: String, state: State<'_, AppState>) -> Result<DataSource, AppError> {
    let ds = {
        let sources = state.data_sources.lock();
        sources
            .get(&id)
            .cloned()
            .ok_or_else(|| AppError::FileError("Data source not found".to_string()))?
    };

    if ds.kind == "external" {
        return Ok(ds);
    }

    let view_name = ds
        .view_name
        .as_deref()
        .ok_or_else(|| AppError::FileError("Data source has no view name".into()))?;

    let connector = {
        let connectors = state.connectors.lock();
        connectors
            .get(&ds.connector_id)
            .cloned()
            .ok_or_else(|| AppError::FileError("Connector not found".into()))?
    };

    let path = connector
        .config
        .path
        .as_deref()
        .ok_or_else(|| AppError::FileError("Connector missing path".into()))?;
    let format = connector
        .config
        .format
        .as_deref()
        .ok_or_else(|| AppError::FileError("Connector missing format".into()))?;

    let (_, duckdb_ref) = format_to_ref(path, format)?;

    let is_local = !path.starts_with("s3://")
        && !path.starts_with("gcs://")
        && !path.starts_with("http://")
        && !path.starts_with("https://");

    if ds.kind == "table" && is_local && !Path::new(path).exists() {
        return Err(AppError::FileError(format!(
            "Source file not found: {}. Materialized table was not modified.",
            path
        )));
    }

    let conn = state.conn.lock();

    if ds.kind == "table" {
        let probe_sql = format!("SELECT 1 FROM {} LIMIT 1", duckdb_ref);
        conn.prepare(&probe_sql)
            .and_then(|mut s| {
                let _: Vec<duckdb::arrow::record_batch::RecordBatch> =
                    s.query_arrow(duckdb::params![])?.collect();
                Ok(())
            })
            .map_err(|e| AppError::FileError(format!(
                "Source is unreachable: {}. Materialized table was not modified.",
                e
            )))?;

        let sql = format!(
            "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM {}",
            view_name, duckdb_ref
        );
        conn.execute_batch(&sql)?;
    } else {
        let sql = format!(
            "CREATE OR REPLACE VIEW \"{}\" AS SELECT * FROM {}",
            view_name, duckdb_ref
        );
        conn.execute_batch(&sql)?;
    }

    let quoted = format!("\"{}\"", view_name);
    let schema = describe_ref(&conn, &quoted)?;
    let row_count = count_ref(&conn, &quoted);

    drop(conn);

    let mut sources = state.data_sources.lock();
    if let Some(existing) = sources.get_mut(&id) {
        existing.schema = schema;
        existing.row_count = row_count;
    }
    let updated = sources.get(&id).cloned().unwrap_or(ds);
    drop(sources);

    catalog::save_state_catalog(&state);

    Ok(updated)
}

#[tauri::command]
pub async fn download_url(
    app: tauri::AppHandle,
    url: String,
) -> Result<String, AppError> {
    use std::path::PathBuf;

    let file_name = url
        .rsplit('/')
        .next()
        .and_then(|s| {
            let s = s.split('?').next().unwrap_or(s);
            if s.is_empty() { None } else { Some(s.to_string()) }
        })
        .unwrap_or_else(|| format!("download_{}", uuid::Uuid::new_v4()));

    let downloads_dir: PathBuf = app
        .path()
        .app_data_dir()
        .map_err(|e| AppError::FileError(format!("resolve app data dir: {}", e)))?
        .join("downloads");

    tokio::fs::create_dir_all(&downloads_dir)
        .await
        .map_err(|e| AppError::FileError(format!("create downloads dir: {}", e)))?;

    let dest = downloads_dir.join(&file_name);

    let resp = reqwest::get(&url)
        .await
        .map_err(|e| AppError::FileError(format!("HTTP request failed: {}", e)))?;

    if !resp.status().is_success() {
        return Err(AppError::FileError(format!(
            "HTTP {} for {}",
            resp.status(),
            url
        )));
    }

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| AppError::FileError(format!("reading response body: {}", e)))?;

    tokio::fs::write(&dest, &bytes)
        .await
        .map_err(|e| AppError::FileError(format!("write file: {}", e)))?;

    Ok(dest.to_string_lossy().to_string())
}
