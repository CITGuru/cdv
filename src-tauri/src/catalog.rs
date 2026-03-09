use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::state::{ConnectionInfo, DataSource};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Catalog {
    pub data_sources: Vec<DataSource>,
    pub connections: Vec<ConnectionRecord>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectionRecord {
    pub id: String,
    pub name: String,
    pub provider: String,
    pub endpoint: Option<String>,
    pub bucket: String,
    pub region: String,
    pub prefix: Option<String>,
    pub account_id: Option<String>,
}

pub fn load_catalog(path: &Path) -> Catalog {
    let Ok(data) = std::fs::read_to_string(path) else {
        return Catalog::default();
    };
    serde_json::from_str(&data).unwrap_or_else(|_| Catalog::default())
}

pub fn save_catalog(path: &Path, catalog: &Catalog) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_string_pretty(catalog).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    std::fs::write(path, data)
}

pub fn catalog_from_state(
    data_sources: &HashMap<String, DataSource>,
    connections: &HashMap<String, ConnectionInfo>,
) -> Catalog {
    Catalog {
        data_sources: data_sources.values().cloned().collect(),
        connections: connections.values().map(ConnectionInfo::to_record).collect(),
    }
}
