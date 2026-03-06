use std::collections::HashMap;

use duckdb::Connection;
use parking_lot::Mutex;
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Serialize, Debug, Clone)]
pub struct DatasetInfo {
    pub id: String,
    pub name: String,
    pub path: String,
    pub source_type: String,
    pub format: String,
    pub schema: Vec<ColumnInfo>,
    pub row_count: Option<u64>,
    pub duckdb_ref: String,
}

pub struct AppState {
    pub conn: Mutex<Connection>,
    pub datasets: Mutex<HashMap<String, DatasetInfo>>,
}

impl AppState {
    pub fn new() -> Self {
        let conn = Connection::open_in_memory().expect("Failed to open DuckDB in-memory connection");
        AppState {
            conn: Mutex::new(conn),
            datasets: Mutex::new(HashMap::new()),
        }
    }
}
