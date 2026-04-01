use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

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
    #[serde(rename = "ducklake")]
    DuckLake,
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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
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
            | ConnectorType::R2
            | ConnectorType::DuckLake => &[Driver::DuckDB],
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

// ──────────────────────────── ETL types ────────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SyncStrategy {
    #[serde(rename = "full")]
    Full,
    #[serde(rename = "incremental")]
    Incremental,
    #[serde(rename = "append")]
    Append,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum JobStatus {
    #[serde(rename = "idle")]
    Idle,
    #[serde(rename = "running")]
    Running,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "cancelled")]
    Cancelled,
    #[serde(rename = "partial")]
    Partial,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum TableStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "running")]
    Running,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "skipped")]
    Skipped,
    #[serde(rename = "failed")]
    Failed,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableSyncState {
    pub schema_name: String,
    pub table_name: String,
    pub status: TableStatus,
    #[serde(default)]
    pub rows_synced: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub completed_at: Option<String>,
    #[serde(default)]
    pub replication_key: Option<String>,
    #[serde(default)]
    pub replication_value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EtlJob {
    pub id: String,
    pub name: String,
    pub source_connector_id: String,
    pub target_connector_id: String,
    pub strategy: SyncStrategy,
    #[serde(default)]
    pub include_schemas: Option<Vec<String>>,
    #[serde(default)]
    pub exclude_tables: Option<Vec<String>>,
    #[serde(default = "default_true")]
    pub skip_views: bool,
    #[serde(default)]
    pub batch_size: Option<u64>,
    pub status: JobStatus,
    #[serde(default)]
    pub table_states: Vec<TableSyncState>,
    pub created_at: String,
    #[serde(default)]
    pub last_run_at: Option<String>,
    #[serde(default)]
    pub last_completed_at: Option<String>,
    #[serde(default)]
    pub total_rows_synced: u64,
    #[serde(default)]
    pub run_count: u32,
}

fn default_true() -> bool {
    true
}

/// Non-default database attached for browse/import; persisted for startup rehydration.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SecondaryAttach {
    pub database: String,
    pub attach_alias: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
}

/// Catalog bucket key for single-database connectors (SQLite, DuckDB, files, etc.).
pub const SINGLE_DB_CATALOG_KEY: &str = "_";

/// Inner map key is database name (or [`SINGLE_DB_CATALOG_KEY`] for non-server connectors).
pub type CatalogByDatabase = HashMap<String, Vec<CatalogEntry>>;

/// Snowflake stores unquoted identifiers uppercased; `INFORMATION_SCHEMA` lists match that.
/// Using the same normalization for catalog keys and `database_names` keeps the sidebar tree aligned.
pub fn normalize_snowflake_database_name(db: &str) -> String {
    let t = db.trim();
    if t.is_empty() {
        SINGLE_DB_CATALOG_KEY.to_string()
    } else {
        t.to_uppercase()
    }
}

pub fn catalog_db_key_for_connector(c: &Connector) -> String {
    match c.connector_type {
        ConnectorType::Snowflake => match &c.config.database {
            Some(s) if !s.trim().is_empty() => normalize_snowflake_database_name(s),
            _ => SINGLE_DB_CATALOG_KEY.to_string(),
        },
        ConnectorType::PostgreSQL => c
            .config
            .database
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| SINGLE_DB_CATALOG_KEY.to_string()),
        _ => SINGLE_DB_CATALOG_KEY.to_string(),
    }
}

pub fn is_multi_database_connector(c: &Connector) -> bool {
    matches!(
        c.connector_type,
        ConnectorType::PostgreSQL | ConnectorType::Snowflake
    )
}

pub struct AppState {
    /// Primary connection for user queries, DDL, and data operations.
    pub conn: Mutex<Connection>,
    /// Secondary connection for metadata operations (introspection, test)
    /// so they don't block the primary query path.
    pub meta_conn: Mutex<Connection>,
    pub data_sources: Mutex<HashMap<String, DataSource>>,
    pub connectors: Mutex<HashMap<String, Connector>>,
    /// Per-connector, per-database table/view catalog (default DB + connected secondaries).
    pub connector_catalogs_by_db: Mutex<HashMap<String, CatalogByDatabase>>,
    /// Last-fetched database name lists for PostgreSQL / Snowflake.
    pub connector_database_names: Mutex<HashMap<String, Vec<String>>>,
    /// Secondary attaches to recreate on startup (Snowflake includes secret per DB).
    pub connector_secondary_attaches: Mutex<HashMap<String, Vec<SecondaryAttach>>>,
    pub catalog_path: PathBuf,
    pub settings_path: PathBuf,
    #[allow(clippy::type_complexity)]
    pub settings_cache: Mutex<Option<crate::settings::Settings>>,
    pub graph_enabled: Mutex<bool>,
    pub etl_jobs: Mutex<HashMap<String, EtlJob>>,
    pub etl_cancel_flag: Arc<AtomicBool>,
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
        let meta_conn = conn
            .try_clone()
            .expect("Failed to clone DuckDB connection for metadata");
        AppState {
            conn: Mutex::new(conn),
            meta_conn: Mutex::new(meta_conn),
            data_sources: Mutex::new(HashMap::new()),
            connectors: Mutex::new(HashMap::new()),
            connector_catalogs_by_db: Mutex::new(HashMap::new()),
            connector_database_names: Mutex::new(HashMap::new()),
            connector_secondary_attaches: Mutex::new(HashMap::new()),
            catalog_path: PathBuf::new(),
            settings_path: PathBuf::new(),
            settings_cache: Mutex::new(None),
            graph_enabled: Mutex::new(graph_ok),
            etl_jobs: Mutex::new(HashMap::new()),
            etl_cancel_flag: Arc::new(AtomicBool::new(false)),
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
        let meta_conn = conn.try_clone()?;
        Ok(AppState {
            conn: Mutex::new(conn),
            meta_conn: Mutex::new(meta_conn),
            data_sources: Mutex::new(HashMap::new()),
            connectors: Mutex::new(HashMap::new()),
            connector_catalogs_by_db: Mutex::new(HashMap::new()),
            connector_database_names: Mutex::new(HashMap::new()),
            connector_secondary_attaches: Mutex::new(HashMap::new()),
            catalog_path,
            settings_path,
            settings_cache: Mutex::new(None),
            graph_enabled: Mutex::new(graph_ok),
            etl_jobs: Mutex::new(HashMap::new()),
            etl_cancel_flag: Arc::new(AtomicBool::new(false)),
        })
    }
}
