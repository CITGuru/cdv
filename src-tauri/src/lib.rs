mod cloud_connector;
mod dataset_manager;
mod error;
mod export_service;
mod query_engine;
mod state;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .manage(state::AppState::new())
        .invoke_handler(tauri::generate_handler![
            dataset_manager::register_dataset,
            dataset_manager::get_schema,
            dataset_manager::get_preview,
            query_engine::run_query,
            query_engine::run_paginated_query,
            query_engine::stream_query,
            cloud_connector::connect_s3,
            cloud_connector::list_bucket_files,
            cloud_connector::open_remote_dataset,
            export_service::export_data,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
