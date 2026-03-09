use std::collections::HashMap;
use std::path::PathBuf;

use duckdb::Connection;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    /// DuckDB DESCRIBE "key" column: e.g. "PRI", "UNI", or empty
    #[serde(default)]
    pub key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataSource {
    pub id: String,
    pub name: String,
    pub view_name: String,
    pub path: String,
    pub source_type: String,
    pub format: String,
    pub schema: Vec<ColumnInfo>,
    pub row_count: Option<u64>,
    pub connection_id: Option<String>,
    /// "view" = view over file; "table" = materialized table
    #[serde(default = "default_kind")]
    pub kind: String,
    /// User-chosen primary key column name (metadata; e.g. for row identity or future PK constraint)
    #[serde(default)]
    pub primary_key_column: Option<String>,
}

fn default_kind() -> String {
    "view".to_string()
}

#[derive(Serialize, Debug, Clone)]
pub struct ConnectionInfo {
    pub id: String,
    pub name: String,
    pub provider: String,
    pub endpoint: Option<String>,
    pub bucket: String,
    pub region: String,
    pub prefix: Option<String>,
    pub account_id: Option<String>,
    #[serde(skip_serializing)]
    pub secret_name: String,
}

impl ConnectionInfo {
    pub fn to_record(&self) -> crate::catalog::ConnectionRecord {
        crate::catalog::ConnectionRecord {
            id: self.id.clone(),
            name: self.name.clone(),
            provider: self.provider.clone(),
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            prefix: self.prefix.clone(),
            account_id: self.account_id.clone(),
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct FilePreview {
    pub format: String,
    pub schema: Vec<ColumnInfo>,
    pub row_count: Option<u64>,
    pub preview_data: Vec<u8>,
}

pub struct AppState {
    pub conn: Mutex<Connection>,
    pub data_sources: Mutex<HashMap<String, DataSource>>,
    pub connections: Mutex<HashMap<String, ConnectionInfo>>,
    pub catalog_path: PathBuf,
    pub settings_path: PathBuf,
    #[allow(clippy::type_complexity)]
    pub settings_cache: Mutex<Option<crate::settings::Settings>>,
}

impl AppState {
    /// Creates in-memory state (for tests or when paths not yet available).
    pub fn new_in_memory() -> Self {
        let conn =
            Connection::open_in_memory().expect("Failed to open DuckDB in-memory connection");
        conn.execute_batch("INSTALL excel; LOAD excel;")
            .expect("Failed to load Excel extension");
        AppState {
            conn: Mutex::new(conn),
            data_sources: Mutex::new(HashMap::new()),
            connections: Mutex::new(HashMap::new()),
            catalog_path: PathBuf::new(),
            settings_path: PathBuf::new(),
            settings_cache: Mutex::new(None),
        }
    }

    /// Creates state with persistent DuckDB and paths for catalog/settings.
    pub fn new_persistent(
        db_path: PathBuf,
        catalog_path: PathBuf,
        settings_path: PathBuf,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&db_path)?;
        conn.execute_batch("INSTALL excel; LOAD excel;")?;
        Ok(AppState {
            conn: Mutex::new(conn),
            data_sources: Mutex::new(HashMap::new()),
            connections: Mutex::new(HashMap::new()),
            catalog_path,
            settings_path,
            settings_cache: Mutex::new(None),
        })
    }
}
