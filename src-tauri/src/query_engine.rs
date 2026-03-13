use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use duckdb::arrow::datatypes::Schema;
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

#[tauri::command]
pub fn run_query(sql: String, state: State<'_, AppState>) -> Result<Vec<u8>, AppError> {
    let conn = state.conn.lock();

    if is_select_like(&sql) {
        let mut stmt = conn.prepare(&sql)?;
        let frames = stmt.query_arrow(params![])?;
        let batches: Vec<RecordBatch> = frames.collect();

        if batches.is_empty() {
            return Ok(Vec::new());
        }
        return batches_to_ipc(&batches[0].schema(), &batches);
    }

    conn.execute_batch(&sql)
        .map_err(|e| AppError::QueryError(e.to_string()))?;
    Ok(Vec::new())
}

#[tauri::command]
pub fn run_paginated_query(args: RunPaginatedQueryArgs, state: State<'_, AppState>) -> Result<Vec<u8>, AppError> {
    let RunPaginatedQueryArgs { sql, page, page_size } = args;
    if !is_select_like(&sql) {
        return run_query(sql, state);
    }

    let offset = page * page_size;
    let paginated_sql = format!(
        "SELECT * FROM ({}) AS _sub LIMIT {} OFFSET {}",
        sql, page_size, offset
    );

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
