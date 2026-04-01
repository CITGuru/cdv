use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::state::{
    normalize_snowflake_database_name, AppState, CatalogByDatabase, Connector, ConnectorConfig,
    ConnectorType, DataSource, Driver, EtlJob, SecondaryAttach, SINGLE_DB_CATALOG_KEY,
};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Catalog {
    #[serde(default)]
    pub connectors: Vec<Connector>,
    #[serde(default)]
    pub data_sources: Vec<DataSource>,
    /// Legacy flat catalogs (migrated into `connector_catalogs_by_db` on load).
    #[serde(default)]
    pub connector_catalogs: HashMap<String, Vec<crate::state::CatalogEntry>>,
    #[serde(default)]
    pub connector_catalogs_by_db: HashMap<String, CatalogByDatabase>,
    #[serde(default)]
    pub connector_database_names: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub connector_secondary_attaches: HashMap<String, Vec<SecondaryAttach>>,
    #[serde(default)]
    pub etl_jobs: Vec<EtlJob>,
}

// Legacy types for migration from the old catalog format.
#[derive(Deserialize, Debug, Clone)]
struct LegacyCatalog {
    #[serde(default)]
    data_sources: Vec<LegacyDataSource>,
    #[serde(default)]
    connections: Vec<LegacyConnectionRecord>,
}

#[derive(Deserialize, Debug, Clone)]
struct LegacyDataSource {
    id: String,
    name: String,
    view_name: String,
    path: String,
    #[serde(default)]
    #[allow(dead_code)]
    source_type: String,
    format: String,
    #[serde(default)]
    schema: Vec<crate::state::ColumnInfo>,
    row_count: Option<u64>,
    connection_id: Option<String>,
    #[serde(default = "default_kind")]
    kind: String,
    #[serde(default)]
    primary_key_column: Option<String>,
}

fn default_kind() -> String {
    "view".to_string()
}

#[derive(Deserialize, Debug, Clone)]
struct LegacyConnectionRecord {
    id: String,
    name: String,
    provider: String,
    endpoint: Option<String>,
    bucket: String,
    region: String,
    prefix: Option<String>,
    account_id: Option<String>,
}

fn default_db_key_for_connector_id(connectors: &[Connector], id: &str) -> String {
    connectors
        .iter()
        .find(|c| c.id == id)
        .map(crate::state::catalog_db_key_for_connector)
        .unwrap_or_else(|| crate::state::SINGLE_DB_CATALOG_KEY.to_string())
}

/// Move legacy flat `connector_catalogs` into `connector_catalogs_by_db` when needed.
/// Fix Snowflake rows where `database_names` (from server) used different casing than catalog keys.
pub fn normalize_snowflake_persisted_catalog(cat: &mut Catalog) {
    for connector in &cat.connectors {
        if connector.connector_type != ConnectorType::Snowflake {
            continue;
        }
        let id = connector.id.clone();

        if let Some(m) = cat.connector_catalogs_by_db.get_mut(&id) {
            let old: CatalogByDatabase = std::mem::take(m);
            for (k, v) in old {
                let nk = if k == SINGLE_DB_CATALOG_KEY {
                    k
                } else {
                    normalize_snowflake_database_name(&k)
                };
                m.entry(nk)
                    .and_modify(|existing| {
                        if v.len() > existing.len() {
                            *existing = v.clone();
                        }
                    })
                    .or_insert(v);
            }
        }

        if let Some(names) = cat.connector_database_names.get_mut(&id) {
            let mut v: Vec<String> = names
                .iter()
                .filter(|s| !s.is_empty())
                .map(|s| normalize_snowflake_database_name(s))
                .collect();
            v.sort();
            v.dedup();
            *names = v;
        }

        if let Some(sec) = cat.connector_secondary_attaches.get_mut(&id) {
            for s in sec.iter_mut() {
                if s.database != SINGLE_DB_CATALOG_KEY {
                    s.database = normalize_snowflake_database_name(&s.database);
                }
            }
        }
    }
}

pub fn migrate_flat_catalogs(cat: &mut Catalog) {
    let flat = std::mem::take(&mut cat.connector_catalogs);
    for (id, entries) in flat {
        if entries.is_empty() {
            continue;
        }
        let inner = cat.connector_catalogs_by_db.entry(id.clone()).or_default();
        if inner.values().any(|v| !v.is_empty()) {
            continue;
        }
        let db_key = default_db_key_for_connector_id(&cat.connectors, &id);
        inner.insert(db_key, entries);
    }
}

pub fn load_catalog(path: &Path) -> Catalog {
    let Ok(data) = std::fs::read_to_string(path) else {
        return Catalog::default();
    };

    if let Ok(mut catalog) = serde_json::from_str::<Catalog>(&data) {
        if !catalog.connectors.is_empty() || catalog.data_sources.is_empty() {
            migrate_flat_catalogs(&mut catalog);
            normalize_snowflake_persisted_catalog(&mut catalog);
            return catalog;
        }
    }

    let Ok(legacy) = serde_json::from_str::<LegacyCatalog>(&data) else {
        return Catalog::default();
    };

    migrate_legacy(legacy)
}

fn migrate_legacy(legacy: LegacyCatalog) -> Catalog {
    let mut connectors: Vec<Connector> = Vec::new();
    let mut data_sources: Vec<DataSource> = Vec::new();
    let mut cloud_id_map: HashMap<String, String> = HashMap::new();

    for rec in &legacy.connections {
        let ct = match rec.provider.as_str() {
            "gcp" => ConnectorType::GCS,
            "cloudflare" => ConnectorType::R2,
            _ => ConnectorType::S3,
        };
        let connector = Connector {
            id: rec.id.clone(),
            name: rec.name.clone(),
            connector_type: ct,
            config: ConnectorConfig {
                bucket: Some(rec.bucket.clone()),
                region: Some(rec.region.clone()),
                endpoint: rec.endpoint.clone(),
                prefix: rec.prefix.clone(),
                account_id: rec.account_id.clone(),
                ..Default::default()
            },
            alias: None,
            secret_name: Some(format!("cdv_{}", rec.id.replace('-', "_"))),
        };
        cloud_id_map.insert(rec.id.clone(), connector.id.clone());
        connectors.push(connector);
    }

    let mut file_connectors: HashMap<String, String> = HashMap::new();

    for ds in &legacy.data_sources {
        let connector_id = if let Some(cid) = &ds.connection_id {
            if let Some(mapped) = cloud_id_map.get(cid) {
                mapped.clone()
            } else {
                cid.clone()
            }
        } else {
            let key = format!("{}:{}", ds.path, ds.format);
            if let Some(existing_id) = file_connectors.get(&key) {
                existing_id.clone()
            } else {
                let fc_id = uuid::Uuid::new_v4().to_string();
                let fc = Connector {
                    id: fc_id.clone(),
                    name: ds.name.clone(),
                    connector_type: ConnectorType::LocalFile,
                    config: ConnectorConfig {
                        path: Some(ds.path.clone()),
                        format: Some(ds.format.clone()),
                        ..Default::default()
                    },
                    alias: None,
                    secret_name: None,
                };
                connectors.push(fc);
                file_connectors.insert(key, fc_id.clone());
                fc_id
            }
        };

        let qualified_name = format!("\"{}\"", ds.view_name);

        data_sources.push(DataSource {
            id: ds.id.clone(),
            name: ds.name.clone(),
            connector_id,
            qualified_name,
            view_name: Some(ds.view_name.clone()),
            schema: ds.schema.clone(),
            row_count: ds.row_count,
            kind: ds.kind.clone(),
            primary_key_column: ds.primary_key_column.clone(),
            driver: Driver::DuckDB,
        });
    }

    Catalog {
        connectors,
        data_sources,
        connector_catalogs: HashMap::new(),
        connector_catalogs_by_db: HashMap::new(),
        connector_database_names: HashMap::new(),
        connector_secondary_attaches: HashMap::new(),
        etl_jobs: Vec::new(),
    }
}

pub fn save_catalog(path: &Path, catalog: &Catalog) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_string_pretty(catalog)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(path, data)
}

pub fn catalog_from_state(
    connectors: &HashMap<String, Connector>,
    data_sources: &HashMap<String, DataSource>,
    connector_catalogs_by_db: &HashMap<String, CatalogByDatabase>,
    connector_database_names: &HashMap<String, Vec<String>>,
    connector_secondary_attaches: &HashMap<String, Vec<SecondaryAttach>>,
    etl_jobs: &HashMap<String, EtlJob>,
) -> Catalog {
    Catalog {
        connectors: connectors.values().cloned().collect(),
        data_sources: data_sources.values().cloned().collect(),
        connector_catalogs: HashMap::new(),
        connector_catalogs_by_db: connector_catalogs_by_db.clone(),
        connector_database_names: connector_database_names.clone(),
        connector_secondary_attaches: connector_secondary_attaches.clone(),
        etl_jobs: etl_jobs.values().cloned().collect(),
    }
}

pub fn save_state_catalog(state: &AppState) {
    let catalog = catalog_from_state(
        &state.connectors.lock(),
        &state.data_sources.lock(),
        &state.connector_catalogs_by_db.lock(),
        &state.connector_database_names.lock(),
        &state.connector_secondary_attaches.lock(),
        &state.etl_jobs.lock(),
    );
    save_catalog(&state.catalog_path, &catalog).ok();
}
