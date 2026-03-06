use std::path::Path;

use duckdb::arrow::record_batch::RecordBatch;
use duckdb::params;
use tauri::State;

use crate::error::AppError;
use crate::state::{AppState, ColumnInfo, DatasetInfo};

fn detect_format(path: &str) -> Result<(String, String), AppError> {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match ext.as_str() {
        "csv" => Ok(("csv".to_string(), format!("read_csv_auto('{}')", path))),
        "tsv" => Ok(("tsv".to_string(), format!("read_csv_auto('{}', delim='\\t')", path))),
        "json" => Ok(("json".to_string(), format!("read_json_auto('{}')", path))),
        "jsonl" => Ok(("jsonl".to_string(), format!("read_json_auto('{}')", path))),
        "parquet" => Ok(("parquet".to_string(), format!("read_parquet('{}')", path))),
        "arrow" | "ipc" => Ok(("arrow_ipc".to_string(), format!("'{}'", path))),
        _ => Err(AppError::FileError(format!("Unsupported file format: .{}", ext))),
    }
}

#[tauri::command]
pub fn register_dataset(path: String, state: State<'_, AppState>) -> Result<DatasetInfo, AppError> {
    let (format, duckdb_ref) = detect_format(&path)?;

    let conn = state.conn.lock();

    let describe_sql = format!("DESCRIBE SELECT * FROM {}", duckdb_ref);
    let mut stmt = conn.prepare(&describe_sql)?;
    let schema: Vec<ColumnInfo> = stmt
        .query_map(params![], |row| {
            Ok(ColumnInfo {
                name: row.get(0)?,
                data_type: row.get(1)?,
                nullable: {
                    let val: String = row.get(3)?;
                    val == "YES"
                },
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    let count_sql = format!("SELECT COUNT(*) FROM {}", duckdb_ref);
    let row_count: Option<u64> = conn
        .query_row(&count_sql, params![], |row| row.get(0))
        .ok();

    let name = Path::new(&path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    let id = uuid::Uuid::new_v4().to_string();

    let dataset = DatasetInfo {
        id: id.clone(),
        name,
        path: path.clone(),
        source_type: "local".to_string(),
        format,
        schema,
        row_count,
        duckdb_ref,
    };

    state.datasets.lock().insert(id, dataset.clone());

    Ok(dataset)
}

#[tauri::command]
pub fn get_schema(dataset_id: String, state: State<'_, AppState>) -> Result<Vec<ColumnInfo>, AppError> {
    let datasets = state.datasets.lock();
    let dataset = datasets
        .get(&dataset_id)
        .ok_or_else(|| AppError::FileError(format!("Dataset not found: {}", dataset_id)))?;
    Ok(dataset.schema.clone())
}

#[tauri::command]
pub fn get_preview(dataset_id: String, state: State<'_, AppState>) -> Result<Vec<u8>, AppError> {
    let duckdb_ref = {
        let datasets = state.datasets.lock();
        let dataset = datasets
            .get(&dataset_id)
            .ok_or_else(|| AppError::FileError(format!("Dataset not found: {}", dataset_id)))?;
        dataset.duckdb_ref.clone()
    };

    let conn = state.conn.lock();
    let sql = format!("SELECT * FROM {} LIMIT 100", duckdb_ref);
    let mut stmt = conn.prepare(&sql)?;
    let frames = stmt.query_arrow(params![])?;
    let batches: Vec<RecordBatch> = frames.collect();

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    crate::query_engine::batches_to_ipc(&batches[0].schema(), &batches)
}
