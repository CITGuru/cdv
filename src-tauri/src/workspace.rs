use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PersistedTab {
    Data {
        id: String,
        #[serde(rename = "dataSourceId")]
        data_source_id: String,
        #[serde(rename = "viewMode")]
        view_mode: Option<String>,
    },
    Query {
        id: String,
        name: String,
        #[serde(rename = "initialSql")]
        initial_sql: Option<String>,
        #[serde(rename = "autoExecute")]
        auto_execute: Option<bool>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct PersistedWorkspace {
    pub open_tabs: Vec<PersistedTab>,
    pub active_tab_id: Option<String>,
}

fn workspace_path(settings_path: &Path) -> std::path::PathBuf {
    settings_path
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join("workspace.json")
}

pub fn load_workspace(settings_path: &Path) -> PersistedWorkspace {
    let path = workspace_path(settings_path);
    let Ok(data) = std::fs::read_to_string(&path) else {
        return PersistedWorkspace::default();
    };
    serde_json::from_str(&data).unwrap_or_else(|_| PersistedWorkspace::default())
}

#[tauri::command]
pub fn get_persisted_tabs(state: tauri::State<'_, crate::state::AppState>) -> PersistedWorkspace {
    load_workspace(&state.settings_path)
}

#[tauri::command]
pub fn set_persisted_tabs(
    workspace: PersistedWorkspace,
    state: tauri::State<'_, crate::state::AppState>,
) -> Result<(), String> {
    save_workspace(&state.settings_path, &workspace).map_err(|e| e.to_string())
}

pub fn save_workspace(settings_path: &Path, workspace: &PersistedWorkspace) -> Result<(), std::io::Error> {
    let path = workspace_path(settings_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_string_pretty(workspace).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    std::fs::write(path, data)
}
