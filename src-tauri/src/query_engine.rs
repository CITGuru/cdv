use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use duckdb::arrow::datatypes::Schema;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::params;
use tauri::{Emitter, State};

use crate::error::AppError;
use crate::state::AppState;

pub fn batches_to_ipc(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<Vec<u8>, AppError> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

#[tauri::command]
pub fn run_query(sql: String, state: State<'_, AppState>) -> Result<Vec<u8>, AppError> {
    let batches: Vec<RecordBatch> = {
        let conn = state.conn.lock();
        let mut stmt = conn.prepare(&sql)?;
        let frames = stmt.query_arrow(params![])?;
        frames.collect()
    };

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    batches_to_ipc(&batches[0].schema(), &batches)
}

#[tauri::command]
pub fn run_paginated_query(
    sql: String,
    page: u32,
    page_size: u32,
    state: State<'_, AppState>,
) -> Result<Vec<u8>, AppError> {
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
