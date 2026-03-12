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
    #[serde(default)]
    pub key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ConnectorType {
    #[serde(rename = "local_file")]
    LocalFile,
    #[serde(rename = "sqlite")]
    SQLite,
    #[serde(rename = "duckdb")]
    DuckDB,
    #[serde(rename = "postgresql")]
    PostgreSQL,
    #[serde(rename = "snowflake")]
    Snowflake,
    #[serde(rename = "s3")]
    S3,
    #[serde(rename = "gcs")]
    GCS,
    #[serde(rename = "r2")]
    R2,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ConnectorConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip)]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Connector {
    pub id: String,
    pub name: String,
    pub connector_type: ConnectorType,
    pub config: ConnectorConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip)]
    pub secret_name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CatalogEntry {
    pub schema: Option<String>,
    pub name: String,
    pub entry_type: String,
    pub columns: Vec<ColumnInfo>,
    pub row_count: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub enum Driver {
    #[default]
    #[serde(rename = "duckdb")]
    DuckDB,
    #[serde(rename = "chdb")]
    ChDB,
}

impl Driver {
    #[allow(dead_code)]
    pub fn label(&self) -> &'static str {
        match self {
            Driver::DuckDB => "DuckDB",
            Driver::ChDB => "chDB",
        }
    }
}

impl ConnectorType {
    pub fn supported_drivers(&self) -> &[Driver] {
        match self {
            ConnectorType::LocalFile
            | ConnectorType::DuckDB
            | ConnectorType::SQLite
            | ConnectorType::S3
            | ConnectorType::GCS
            | ConnectorType::R2 => &[Driver::DuckDB],
            ConnectorType::PostgreSQL | ConnectorType::Snowflake => &[Driver::DuckDB],
        }
    }

    pub fn default_driver(&self) -> Driver {
        self.supported_drivers()[0].clone()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataSource {
    pub id: String,
    pub name: String,
    pub connector_id: String,
    pub qualified_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_name: Option<String>,
    pub schema: Vec<ColumnInfo>,
    pub row_count: Option<u64>,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub primary_key_column: Option<String>,
    #[serde(default)]
    pub driver: Driver,
}

fn default_kind() -> String {
    "view".to_string()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VertexTableDef {
    pub table_name: String,
    pub key_column: Option<String>,
    pub label: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EdgeTableDef {
    pub table_name: String,
    pub source_key: String,
    pub source_vertex_table: String,
    pub source_vertex_key: String,
    pub destination_key: String,
    pub destination_vertex_table: String,
    pub destination_vertex_key: String,
    pub label: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PropertyGraphInfo {
    pub name: String,
    pub vertex_tables: Vec<String>,
    pub edge_tables: Vec<String>,
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
    pub connectors: Mutex<HashMap<String, Connector>>,
    pub catalog_path: PathBuf,
    pub settings_path: PathBuf,
    #[allow(clippy::type_complexity)]
    pub settings_cache: Mutex<Option<crate::settings::Settings>>,
    pub graph_enabled: Mutex<bool>,
}

impl AppState {
    pub fn new_in_memory() -> Self {
        let conn =
            Connection::open_in_memory().expect("Failed to open DuckDB in-memory connection");
        conn.execute_batch("INSTALL excel; LOAD excel;")
            .expect("Failed to load Excel extension");
        let graph_ok =
            conn.execute_batch("INSTALL duckpgq FROM community; LOAD duckpgq;").is_ok();
        if !graph_ok {
            eprintln!("DuckPGQ extension unavailable (in-memory mode)");
        }
        AppState {
            conn: Mutex::new(conn),
            data_sources: Mutex::new(HashMap::new()),
            connectors: Mutex::new(HashMap::new()),
            catalog_path: PathBuf::new(),
            settings_path: PathBuf::new(),
            settings_cache: Mutex::new(None),
            graph_enabled: Mutex::new(graph_ok),
        }
    }

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
        let graph_ok =
            conn.execute_batch("INSTALL duckpgq FROM community; LOAD duckpgq;").is_ok();
        if !graph_ok {
            eprintln!("DuckPGQ extension unavailable");
        }
        Ok(AppState {
            conn: Mutex::new(conn),
            data_sources: Mutex::new(HashMap::new()),
            connectors: Mutex::new(HashMap::new()),
            catalog_path,
            settings_path,
            settings_cache: Mutex::new(None),
            graph_enabled: Mutex::new(graph_ok),
        })
    }
}
