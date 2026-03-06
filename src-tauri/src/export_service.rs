use tauri::State;

use crate::error::AppError;
use crate::state::AppState;

#[tauri::command]
pub fn export_data(
    query: String,
    format: String,
    output_path: String,
    state: State<'_, AppState>,
) -> Result<(), AppError> {
    let duckdb_format = match format.to_lowercase().as_str() {
        "csv" => "csv",
        "parquet" => "parquet",
        "json" => "json",
        _ => return Err(AppError::ExportError(format!("Unsupported export format: {}", format))),
    };

    let sql = format!(
        "COPY ({}) TO '{}' (FORMAT {})",
        query, output_path, duckdb_format
    );

    let conn = state.conn.lock();
    conn.execute_batch(&sql)
        .map_err(|e| AppError::ExportError(e.to_string()))?;

    Ok(())
}
