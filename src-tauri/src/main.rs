// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    if std::env::args().nth(1).as_deref() == Some("--query-worker") {
        cdv_lib::run_query_worker();
        return;
    }
    cdv_lib::run()
}
