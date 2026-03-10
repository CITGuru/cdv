use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::state::{AppState, Connector, ConnectorConfig, ConnectorType, DataSource, Driver};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Catalog {
    #[serde(default)]
    pub connectors: Vec<Connector>,
    #[serde(default)]
    pub data_sources: Vec<DataSource>,
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

pub fn load_catalog(path: &Path) -> Catalog {
    let Ok(data) = std::fs::read_to_string(path) else {
        return Catalog::default();
    };

    if let Ok(catalog) = serde_json::from_str::<Catalog>(&data) {
        if !catalog.connectors.is_empty() || catalog.data_sources.is_empty() {
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
) -> Catalog {
    Catalog {
        connectors: connectors.values().cloned().collect(),
        data_sources: data_sources.values().cloned().collect(),
    }
}

pub fn save_state_catalog(state: &AppState) {
    let catalog = catalog_from_state(&state.connectors.lock(), &state.data_sources.lock());
    save_catalog(&state.catalog_path, &catalog).ok();
}
