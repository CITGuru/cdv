use std::path::Path;

use serde::{Deserialize, Serialize};
use tauri::State;

use crate::state::AppState;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Settings {
    pub sidebar_width: u32,
    pub default_page_size: u32,
    pub max_rows_per_query: u32,
    pub default_export_format: String,
    pub streaming_enabled: bool,
    pub streaming_threshold: Option<u32>,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            sidebar_width: 256,
            default_page_size: 1000,
            max_rows_per_query: 10_000,
            default_export_format: "csv".to_string(),
            streaming_enabled: false,
            streaming_threshold: Some(10_000),
        }
    }
}

pub fn load_settings(path: &Path) -> Settings {
    let Ok(data) = std::fs::read_to_string(path) else {
        return Settings::default();
    };
    serde_json::from_str(&data).unwrap_or_else(|_| Settings::default())
}

pub fn save_settings(path: &Path, settings: &Settings) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_string_pretty(settings).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    std::fs::write(path, data)
}

#[tauri::command]
pub fn get_settings(state: State<'_, AppState>) -> Settings {
    let path = &state.settings_path;
    let mut cache = state.settings_cache.lock();
    if let Some(ref cached) = *cache {
        return cached.clone();
    }
    let settings = load_settings(path);
    *cache = Some(settings.clone());
    settings
}

#[tauri::command]
pub fn set_settings(settings: Settings, state: State<'_, AppState>) -> Result<(), String> {
    let path = state.settings_path.clone();
    save_settings(&path, &settings).map_err(|e| e.to_string())?;
    *state.settings_cache.lock() = Some(settings);
    Ok(())
}
