use std::sync::Arc;

use duckdb::arrow::datatypes::Schema;
use duckdb::arrow::ipc::writer::StreamWriter;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::params;
use serde::Deserialize;
use tauri::{Emitter, State};

use crate::error::AppError;
use crate::state::AppState;

#[derive(Deserialize)]
pub(crate) struct RunPaginatedQueryArgs {
    sql: String,
    page: u32,
    #[serde(rename = "pageSize")]
    page_size: u32,
}

pub fn batches_to_ipc(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<Vec<u8>, AppError> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

fn is_select_like(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let upper: String = trimmed.chars().take(10).collect::<String>().to_uppercase();
    upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("FROM")
        || upper.starts_with("TABLE")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("PRAGMA")
        || upper.starts_with("CALL")
}

/// Try executing a SELECT query in the isolated worker process.
///
/// Returns:
///   `Some(Ok(data))`  – worker succeeded; use the result.
///   `Some(Err(…))`    – worker *crashed* (SIGABRT / broken pipe); the error
///                       is returned to the frontend and we must NOT fall back
///                       to the in-process connection (same data would crash it).
///   `None`            – worker unavailable or returned a non-crash error
///                       (e.g. table not found because the main DB file could
///                       not be ATTACHed). Caller should fall back to the
///                       in-process connection which has the full state.
fn try_worker_query(state: &AppState, sql: &str) -> Option<Result<Vec<u8>, AppError>> {
    let mut guard = state.query_worker.lock();
    let worker = guard.as_mut()?;
    let result = worker.execute(sql);

    match result {
        Ok(data) => Some(Ok(data)),
        Err(ref e) => {
            let is_crash = matches!(e, AppError::QueryError(msg) if msg.contains("crashed"));
            if is_crash {
                eprintln!("[cdv] Query worker crashed, attempting restart…");
                match worker.respawn() {
                    Ok(()) => eprintln!("[cdv] Query worker restarted"),
                    Err(re) => {
                        eprintln!("[cdv] Query worker respawn failed: {re}");
                        *guard = None;
                    }
                }
                Some(Err(AppError::QueryError(
                    "Query execution failed: the query worker process crashed due to a \
                     DuckDB internal error (assertion failure in string compression). \
                     This is a known DuckDB engine bug triggered by certain data. \
                     The worker has been restarted for subsequent queries."
                        .to_string(),
                )))
            } else {
                // Non-crash error (table not found, permission denied, etc.)
                // — fall back to the main in-process connection.
                eprintln!(
                    "[cdv] Worker returned non-crash error, falling back to main connection: {e}"
                );
                None
            }
        }
    }
}

#[tauri::command]
pub fn run_query(sql: String, state: State<'_, AppState>) -> Result<Vec<u8>, AppError> {
    if is_select_like(&sql) {
        if let Some(result) = try_worker_query(&state, &sql) {
            return result;
        }

        let conn = state.conn.lock();
        let mut stmt = conn.prepare(&sql)?;
        let frames = stmt.query_arrow(params![])?;
        let batches: Vec<RecordBatch> = frames.collect();

        if batches.is_empty() {
            return Ok(Vec::new());
        }
        return batches_to_ipc(&batches[0].schema(), &batches);
    }

    let conn = state.conn.lock();
    conn.execute_batch(&sql)
        .map_err(|e| AppError::QueryError(e.to_string()))?;
    Ok(Vec::new())
}

#[tauri::command]
pub fn run_paginated_query(
    args: RunPaginatedQueryArgs,
    state: State<'_, AppState>,
) -> Result<Vec<u8>, AppError> {
    let RunPaginatedQueryArgs {
        sql,
        page,
        page_size,
    } = args;
    if !is_select_like(&sql) {
        return run_query(sql, state);
    }

    let offset = page * page_size;
    let paginated_sql = format!(
        "SELECT * FROM ({}) AS _sub LIMIT {} OFFSET {}",
        sql, page_size, offset
    );

    eprintln!(
        "[cdv] run_paginated_query: page={page} size={page_size} sql={}",
        if sql.len() > 120 { &sql[..120] } else { &sql }
    );

    if let Some(result) = try_worker_query(&state, &paginated_sql) {
        return result;
    }

    // No worker available — direct execution fallback
    let batches: Vec<RecordBatch> = {
        let conn = state.conn.lock();
        let mut stmt = conn.prepare(&paginated_sql)?;
        let frames = stmt.query_arrow(params![])?;
        frames.collect()
    };

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    batches_to_ipc(&batches[0].schema(), &batches)
}

#[tauri::command]
pub fn stream_query(
    sql: String,
    app_handle: tauri::AppHandle,
    state: State<'_, AppState>,
) -> Result<(), AppError> {
    let conn = state.conn.lock();

    if !is_select_like(&sql) {
        conn.execute_batch(&sql)
            .map_err(|e| AppError::QueryError(e.to_string()))?;
        let _ = app_handle.emit("query-complete", ());
        return Ok(());
    }

    eprintln!(
        "[cdv] stream_query: sql={}",
        if sql.len() > 120 { &sql[..120] } else { &sql }
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut frames = stmt.query_arrow(params![])?;

    for batch in frames.by_ref() {
        let ipc_bytes = batches_to_ipc(&batch.schema(), &[batch])?;
        let _ = app_handle.emit("query-chunk", ipc_bytes);
    }

    drop(conn);
    let _ = app_handle.emit("query-complete", ());

    Ok(())
}
