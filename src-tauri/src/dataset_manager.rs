use std::path::Path;

use duckdb::arrow::record_batch::RecordBatch;
use duckdb::params;
use duckdb::Connection;
use tauri::State;

use crate::catalog;
use crate::error::AppError;
use crate::state::{AppState, ColumnInfo, DataSource, FilePreview};

/// Builds (format_name, duckdb_ref) for rehydration. Path can be local or s3://...
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

/// Escape path for use inside single-quoted SQL string (e.g. paths with quotes or backslashes).
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
        "json" => Ok(("json".to_string(), format!("read_json_auto('{}')", path_esc))),
        "jsonl" => Ok(("jsonl".to_string(), format!("read_json_auto('{}')", path_esc))),
        "parquet" => Ok(("parquet".to_string(), format!("read_parquet('{}')", path_esc))),
        "xlsx" => Ok(("xlsx".to_string(), format!("read_xlsx('{}')", path_esc))),
        "arrow" | "ipc" | "arrow_ipc" => Ok(("arrow_ipc".to_string(), format!("'{}'", path_esc))),
        _ => Err(AppError::FileError(format!(
            "Unsupported file format: {}",
            format
        ))),
    }
}

/// DuckDB DESCRIBE returns: column_name (0), column_type (1), null (2), key (3), default (4), extra (5).
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
                    let val: String = row.get(2)?; // "null" column: YES/NO
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

/// Recreates DuckDB views from persisted catalog. Call after opening DB on startup.
/// Tables (materialized) already exist in the DB file; only views are re-created.
pub fn rehydrate_views(conn: &Connection, data_sources: &[DataSource]) -> Result<(), AppError> {
    for ds in data_sources {
        if ds.kind == "table" {
            continue; // table already in DB file
        }
        let duckdb_ref = build_duckdb_ref(&ds.path, &ds.format)?;
        let create_sql = format!(
            "CREATE OR REPLACE VIEW \"{}\" AS SELECT * FROM {}",
            ds.view_name, duckdb_ref
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
    } else if trimmed.chars().next().map_or(true, |c| c.is_numeric()) {
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

    let conn = state.conn.lock();
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
    path: String,
    format: Option<String>,
    connection_id: Option<String>,
    materialize: Option<bool>,
    primary_key_column: Option<String>,
    state: State<'_, AppState>,
) -> Result<DataSource, AppError> {
    let safe_view = sanitize_view_name(&view_name);
    let materialize = materialize.unwrap_or(false);
    let kind = if materialize { "table" } else { "view" };

    let (fmt, duckdb_ref) = match &format {
        Some(f) => format_to_ref(&path, f)?,
        None => detect_format(&path)?,
    };

    let conn = state.conn.lock();

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

    let source_type = if connection_id.is_some() {
        "s3"
    } else {
        "local"
    };

    let id = uuid::Uuid::new_v4().to_string();
    let data_source = DataSource {
        id: id.clone(),
        name,
        view_name: safe_view,
        path,
        source_type: source_type.to_string(),
        format: fmt,
        schema,
        row_count,
        connection_id,
        kind: kind.to_string(),
        primary_key_column: primary_key_column.filter(|s| !s.is_empty()),
    };

    state.data_sources.lock().insert(id, data_source.clone());

    let catalog = catalog::catalog_from_state(
        &*state.data_sources.lock(),
        &*state.connections.lock(),
    );
    catalog::save_catalog(&state.catalog_path, &catalog).ok();

    Ok(data_source)
}

#[tauri::command]
pub fn remove_data_source(id: String, state: State<'_, AppState>) -> Result<(), AppError> {
    let (view_name, kind) = {
        let mut sources = state.data_sources.lock();
        let ds = sources
            .remove(&id)
            .ok_or_else(|| AppError::FileError("Data source not found".to_string()))?;
        (ds.view_name, ds.kind)
    };

    let conn = state.conn.lock();
    if kind == "table" {
        conn.execute_batch(&format!("DROP TABLE IF EXISTS \"{}\"", view_name))?;
    } else {
        conn.execute_batch(&format!("DROP VIEW IF EXISTS \"{}\"", view_name))?;
    }
    drop(conn);

    let catalog = catalog::catalog_from_state(
        &*state.data_sources.lock(),
        &*state.connections.lock(),
    );
    catalog::save_catalog(&state.catalog_path, &catalog).ok();

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
    let view_name = {
        let sources = state.data_sources.lock();
        let ds = sources
            .get(&dataset_id)
            .ok_or_else(|| {
                AppError::FileError(format!("Data source not found: {}", dataset_id))
            })?;
        ds.view_name.clone()
    };

    let conn = state.conn.lock();
    let sql = format!("SELECT * FROM \"{}\" LIMIT 100", view_name);
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
    path: String,
    name: Option<String>,
    view_name: Option<String>,
    format: Option<String>,
    connection_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<DataSource, AppError> {
    let (old_view_name, kind) = {
        let sources = state.data_sources.lock();
        let ds = sources
            .get(&id)
            .ok_or_else(|| AppError::FileError("Data source not found".to_string()))?;
        (ds.view_name.clone(), ds.kind.clone())
    };

    let safe_view = view_name
        .as_ref()
        .map(|v| sanitize_view_name(v))
        .unwrap_or_else(|| sanitize_view_name(&old_view_name));

    let conn = state.conn.lock();

    if kind == "table" {
        // For materialized tables only allow name/view_name changes; rename table in DB if needed
        if safe_view != old_view_name {
            conn.execute_batch(&format!(
                "ALTER TABLE \"{}\" RENAME TO \"{}\"",
                old_view_name, safe_view
            ))?;
        }
        let mut sources = state.data_sources.lock();
        let ds = sources
            .get_mut(&id)
            .ok_or_else(|| AppError::FileError("Data source not found".to_string()))?;
        if let Some(n) = name {
            ds.name = n;
        }
        ds.view_name = safe_view.clone();
        let result = ds.clone();
        drop(sources);
        drop(conn);

        let catalog = catalog::catalog_from_state(
            &*state.data_sources.lock(),
            &*state.connections.lock(),
        );
        catalog::save_catalog(&state.catalog_path, &catalog).ok();

        return Ok(result);
    }

    let (fmt, duckdb_ref) = match &format {
        Some(f) => format_to_ref(&path, f)?,
        None => detect_format(&path)?,
    };

    conn.execute_batch(&format!("DROP VIEW IF EXISTS \"{}\"", old_view_name))?;

    let create_sql = format!(
        "CREATE OR REPLACE VIEW \"{}\" AS SELECT * FROM {}",
        safe_view, duckdb_ref
    );
    conn.execute_batch(&create_sql)?;

    let quoted = format!("\"{}\"", safe_view);
    let schema = describe_ref(&conn, &quoted)?;
    let row_count = count_ref(&conn, &quoted);

    let source_type = if connection_id.is_some() {
        "s3"
    } else {
        "local"
    };

    let mut sources = state.data_sources.lock();
    let ds = sources
        .get_mut(&id)
        .ok_or_else(|| AppError::FileError("Data source not found".to_string()))?;

    ds.path = path;
    ds.view_name = safe_view.clone();
    ds.source_type = source_type.to_string();
    ds.format = fmt;
    ds.schema = schema;
    ds.row_count = row_count;
    ds.connection_id = connection_id.clone();
    if let Some(n) = name {
        ds.name = n;
    }
    let result = ds.clone();
    drop(sources);

    let catalog = catalog::catalog_from_state(
        &*state.data_sources.lock(),
        &*state.connections.lock(),
    );
    catalog::save_catalog(&state.catalog_path, &catalog).ok();

    Ok(result)
}
