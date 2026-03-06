use duckdb::params;
use serde::Deserialize;
use tauri::State;

use crate::error::AppError;
use crate::state::{AppState, ColumnInfo, DatasetInfo};

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub prefix: Option<String>,
}

#[tauri::command]
pub fn connect_s3(config: S3Config, state: State<'_, AppState>) -> Result<(), AppError> {
    let conn = state.conn.lock();

    conn.execute_batch("INSTALL httpfs; LOAD httpfs;")
        .map_err(|e| AppError::AuthError(format!("Failed to load HTTPFS: {}", e)))?;

    conn.execute_batch(&format!("SET s3_region='{}';", config.region))
        .map_err(|e| AppError::AuthError(e.to_string()))?;
    conn.execute_batch(&format!("SET s3_access_key_id='{}';", config.access_key))
        .map_err(|e| AppError::AuthError(e.to_string()))?;
    conn.execute_batch(&format!("SET s3_secret_access_key='{}';", config.secret_key))
        .map_err(|e| AppError::AuthError(e.to_string()))?;

    if let Some(endpoint) = &config.endpoint {
        conn.execute_batch(&format!("SET s3_endpoint='{}';", endpoint))
            .map_err(|e| AppError::AuthError(e.to_string()))?;
        conn.execute_batch("SET s3_url_style='path';")
            .map_err(|e| AppError::AuthError(e.to_string()))?;
    }

    Ok(())
}

#[tauri::command]
pub fn list_bucket_files(
    bucket: String,
    prefix: Option<String>,
    state: State<'_, AppState>,
) -> Result<Vec<String>, AppError> {
    let conn = state.conn.lock();

    let path = match prefix {
        Some(p) => format!("s3://{}/{}/*", bucket, p),
        None => format!("s3://{}/*", bucket),
    };

    let sql = format!("SELECT file FROM glob('{}')", path);
    let mut stmt = conn.prepare(&sql)?;
    let files: Vec<String> = stmt
        .query_map(params![], |row| row.get(0))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(files)
}

#[tauri::command]
pub fn open_remote_dataset(
    s3_path: String,
    state: State<'_, AppState>,
) -> Result<DatasetInfo, AppError> {
    let ext = s3_path
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_lowercase();

    let (format, duckdb_ref) = match ext.as_str() {
        "csv" => ("csv".to_string(), format!("read_csv_auto('{}')", s3_path)),
        "tsv" => ("tsv".to_string(), format!("read_csv_auto('{}', delim='\\t')", s3_path)),
        "json" => ("json".to_string(), format!("read_json_auto('{}')", s3_path)),
        "jsonl" => ("jsonl".to_string(), format!("read_json_auto('{}')", s3_path)),
        "parquet" => ("parquet".to_string(), format!("read_parquet('{}')", s3_path)),
        _ => return Err(AppError::FileError(format!("Unsupported format: .{}", ext))),
    };

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

    let name = s3_path
        .rsplit('/')
        .next()
        .unwrap_or("remote_dataset")
        .to_string();

    let id = uuid::Uuid::new_v4().to_string();

    let dataset = DatasetInfo {
        id: id.clone(),
        name,
        path: s3_path,
        source_type: "s3".to_string(),
        format,
        schema,
        row_count,
        duckdb_ref,
    };

    state.datasets.lock().insert(id, dataset.clone());

    Ok(dataset)
}
