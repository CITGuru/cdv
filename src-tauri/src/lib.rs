mod catalog;
mod cloud_connector;
mod dataset_manager;
mod error;
mod export_service;
mod query_engine;
mod settings;
mod state;
mod workspace;

use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            let app_data_dir = app
                .path()
                .app_data_dir()
                .expect("failed to resolve app data dir");
            std::fs::create_dir_all(&app_data_dir).ok();
            let db_path = app_data_dir.join("cdv.duckdb");
            let catalog_path = app_data_dir.join("catalog.json");
            let settings_path = app_data_dir.join("settings.json");

            let state = match state::AppState::new_persistent(
                db_path,
                catalog_path.clone(),
                settings_path.clone(),
            ) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Failed to open persistent DB, using in-memory: {}", e);
                    let mut s = state::AppState::new_in_memory();
                    s.catalog_path = catalog_path;
                    s.settings_path = settings_path;
                    s
                }
            };

            let catalog = catalog::load_catalog(&state.catalog_path);
            if let Err(e) = dataset_manager::rehydrate_views(&*state.conn.lock(), &catalog.data_sources) {
                eprintln!("Rehydrate views warning: {}", e);
            }
            for ds in catalog.data_sources {
                state.data_sources.lock().insert(ds.id.clone(), ds);
            }
            for rec in catalog.connections {
                let secret_name = format!("cdv_{}", rec.id.replace("-", "_"));
                let info = state::ConnectionInfo {
                    id: rec.id.clone(),
                    name: rec.name,
                    provider: rec.provider,
                    endpoint: rec.endpoint,
                    bucket: rec.bucket,
                    region: rec.region,
                    prefix: rec.prefix,
                    account_id: rec.account_id,
                    secret_name,
                };
                state.connections.lock().insert(rec.id, info);
            }

            app.manage(state);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            dataset_manager::preview_file,
            dataset_manager::create_data_source,
            dataset_manager::remove_data_source,
            dataset_manager::list_data_sources,
            dataset_manager::get_schema,
            dataset_manager::get_preview,
            dataset_manager::update_data_source,
            query_engine::run_query,
            query_engine::run_paginated_query,
            query_engine::stream_query,
            cloud_connector::create_connection,
            cloud_connector::remove_connection,
            cloud_connector::list_connections,
            cloud_connector::list_connection_files,
            export_service::export_data,
            settings::get_settings,
            settings::set_settings,
            workspace::get_persisted_tabs,
            workspace::set_persisted_tabs,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
