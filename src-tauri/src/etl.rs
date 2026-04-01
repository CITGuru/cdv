use std::collections::HashSet;
use std::sync::atomic::Ordering;

use duckdb::params;
use duckdb::Connection;
use serde::Serialize;
use tauri::{Emitter, Manager, State};

use crate::catalog;
use crate::connector::{ensure_extension, escape_sql_string, get_ops};
use crate::error::AppError;
use crate::state::{
    AppState, CatalogByDatabase, CatalogEntry, ConnectorType, EtlJob, JobStatus, SecondaryAttach,
    SyncStrategy, TableStatus, TableSyncState,
};

// ──────────────────────────── Event payloads ────────────────────────────

#[derive(Serialize, Clone)]
struct EtlProgressEvent {
    job_id: String,
    phase: String,
    current_table_index: usize,
    total_tables: usize,
    schema_name: String,
    table_name: String,
    status: String,
    rows_synced: Option<u64>,
    error: Option<String>,
    elapsed_ms: u64,
}

#[derive(Serialize, Clone)]
struct EtlCompleteEvent {
    job_id: String,
    status: String,
    tables_migrated: usize,
    tables_failed: usize,
    total_rows: u64,
    elapsed_ms: u64,
}

// ──────────────────────────── Tauri Commands ────────────────────────────

#[tauri::command]
pub fn create_etl_job(
    name: String,
    source_connector_id: String,
    target_connector_id: String,
    strategy: SyncStrategy,
    include_schemas: Option<Vec<String>>,
    exclude_tables: Option<Vec<String>>,
    skip_views: Option<bool>,
    batch_size: Option<u64>,
    state: State<'_, AppState>,
) -> Result<EtlJob, AppError> {
    {
        let connectors = state.connectors.lock();
        let source = connectors
            .get(&source_connector_id)
            .ok_or_else(|| AppError::EtlError("Source connector not found".into()))?;
        if source.connector_type != ConnectorType::PostgreSQL {
            return Err(AppError::EtlError(
                "Source must be a PostgreSQL connector".into(),
            ));
        }
        let target = connectors
            .get(&target_connector_id)
            .ok_or_else(|| AppError::EtlError("Target connector not found".into()))?;
        if target.connector_type != ConnectorType::DuckLake {
            return Err(AppError::EtlError(
                "Target must be a DuckLake connector".into(),
            ));
        }
    }

    let now = chrono::Utc::now().to_rfc3339();
    let job = EtlJob {
        id: uuid::Uuid::new_v4().to_string(),
        name,
        source_connector_id,
        target_connector_id,
        strategy,
        include_schemas,
        exclude_tables,
        skip_views: skip_views.unwrap_or(true),
        batch_size,
        status: JobStatus::Idle,
        table_states: Vec::new(),
        created_at: now,
        last_run_at: None,
        last_completed_at: None,
        total_rows_synced: 0,
        run_count: 0,
    };

    state
        .etl_jobs
        .lock()
        .insert(job.id.clone(), job.clone());
    catalog::save_state_catalog(&state);

    Ok(job)
}

#[tauri::command]
pub fn list_etl_jobs(state: State<'_, AppState>) -> Result<Vec<EtlJob>, AppError> {
    Ok(state.etl_jobs.lock().values().cloned().collect())
}

#[tauri::command]
pub fn get_etl_job(job_id: String, state: State<'_, AppState>) -> Result<EtlJob, AppError> {
    state
        .etl_jobs
        .lock()
        .get(&job_id)
        .cloned()
        .ok_or_else(|| AppError::EtlError("ETL job not found".into()))
}

#[tauri::command]
pub fn delete_etl_job(job_id: String, state: State<'_, AppState>) -> Result<(), AppError> {
    let mut jobs = state.etl_jobs.lock();
    if let Some(job) = jobs.get(&job_id) {
        if job.status == JobStatus::Running {
            return Err(AppError::EtlError(
                "Cannot delete a running job. Cancel it first.".into(),
            ));
        }
    }
    jobs.remove(&job_id)
        .ok_or_else(|| AppError::EtlError("ETL job not found".into()))?;
    drop(jobs);
    catalog::save_state_catalog(&state);
    Ok(())
}

#[tauri::command]
pub fn cancel_etl_job(state: State<'_, AppState>) -> Result<(), AppError> {
    state.etl_cancel_flag.store(true, Ordering::SeqCst);
    Ok(())
}

#[tauri::command]
pub fn preview_etl_job(
    source_connector_id: String,
    include_schemas: Option<Vec<String>>,
    exclude_tables: Option<Vec<String>>,
    skip_views: Option<bool>,
    state: State<'_, AppState>,
) -> Result<Vec<CatalogEntry>, AppError> {
    let connector = {
        let connectors = state.connectors.lock();
        connectors
            .get(&source_connector_id)
            .cloned()
            .ok_or_else(|| AppError::EtlError("Source connector not found".into()))?
    };
    if connector.connector_type != ConnectorType::PostgreSQL {
        return Err(AppError::EtlError(
            "Source must be a PostgreSQL connector".into(),
        ));
    }

    let ops = get_ops(&connector.connector_type);
    let conn = state.meta_conn.lock();
    let entries = ops.introspect(&conn, &connector)?;
    drop(conn);

    let skip = skip_views.unwrap_or(true);
    let filtered = filter_entries(&entries, &include_schemas, &exclude_tables, skip);
    Ok(filtered)
}

#[tauri::command]
pub fn run_etl_job(
    job_id: String,
    app_handle: tauri::AppHandle,
    state: State<'_, AppState>,
) -> Result<(), AppError> {
    let mut jobs = state.etl_jobs.lock();
    let job = jobs
        .get_mut(&job_id)
        .ok_or_else(|| AppError::EtlError("ETL job not found".into()))?;

    if job.status == JobStatus::Running {
        return Err(AppError::EtlError("Job is already running".into()));
    }

    job.status = JobStatus::Running;
    job.last_run_at = Some(chrono::Utc::now().to_rfc3339());
    job.run_count += 1;
    let job_snapshot = job.clone();
    drop(jobs);

    let source = {
        let connectors = state.connectors.lock();
        connectors
            .get(&job_snapshot.source_connector_id)
            .cloned()
            .ok_or_else(|| AppError::EtlError("Source connector not found".into()))?
    };
    let target = {
        let connectors = state.connectors.lock();
        connectors
            .get(&job_snapshot.target_connector_id)
            .cloned()
            .ok_or_else(|| AppError::EtlError("Target connector not found".into()))?
    };

    let migration_conn = state
        .conn
        .lock()
        .try_clone()
        .map_err(|e| AppError::EtlError(format!("Failed to clone connection: {}", e)))?;

    let cancel_flag = state.etl_cancel_flag.clone();
    cancel_flag.store(false, Ordering::SeqCst);

    catalog::save_state_catalog(&state);

    // Grab the shared state pieces we need for the background thread.
    // We clone the snapshot maps for catalog persistence, and keep
    // a reference to the shared etl_jobs Mutex via the AppHandle.
    let catalog_path = state.catalog_path.clone();
    let connectors_map = state.connectors.lock().clone();
    let data_sources_map = state.data_sources.lock().clone();
    let connector_catalogs_by_db_map = state.connector_catalogs_by_db.lock().clone();
    let connector_database_names_map = state.connector_database_names.lock().clone();
    let connector_secondary_attaches_map = state.connector_secondary_attaches.lock().clone();

    std::thread::spawn(move || {
        let state_ref: tauri::State<'_, AppState> = app_handle.state();
        let etl_jobs_ref = &state_ref.etl_jobs;

        execute_etl_job(
            &migration_conn,
            job_snapshot,
            source,
            target,
            &app_handle,
            &cancel_flag,
            etl_jobs_ref,
            &catalog_path,
            &connectors_map,
            &data_sources_map,
            &connector_catalogs_by_db_map,
            &connector_database_names_map,
            &connector_secondary_attaches_map,
        );
    });

    Ok(())
}

fn save_catalog_direct(
    catalog_path: &std::path::Path,
    connectors_map: &std::collections::HashMap<String, crate::state::Connector>,
    data_sources_map: &std::collections::HashMap<String, crate::state::DataSource>,
    connector_catalogs_by_db: &std::collections::HashMap<String, CatalogByDatabase>,
    connector_database_names: &std::collections::HashMap<String, Vec<String>>,
    connector_secondary_attaches: &std::collections::HashMap<String, Vec<SecondaryAttach>>,
    etl_jobs_map: &std::collections::HashMap<String, EtlJob>,
) {
    let catalog = catalog::catalog_from_state(
        connectors_map,
        data_sources_map,
        connector_catalogs_by_db,
        connector_database_names,
        connector_secondary_attaches,
        etl_jobs_map,
    );
    catalog::save_catalog(catalog_path, &catalog).ok();
}

// ──────────────────────────── Background executor ────────────────────────────

fn execute_etl_job(
    conn: &Connection,
    mut job: EtlJob,
    source: crate::state::Connector,
    target: crate::state::Connector,
    app_handle: &tauri::AppHandle,
    cancel_flag: &std::sync::atomic::AtomicBool,
    etl_jobs_mutex: &parking_lot::Mutex<std::collections::HashMap<String, EtlJob>>,
    catalog_path: &std::path::Path,
    connectors_map: &std::collections::HashMap<String, crate::state::Connector>,
    data_sources_map: &std::collections::HashMap<String, crate::state::DataSource>,
    connector_catalogs_by_db: &std::collections::HashMap<String, CatalogByDatabase>,
    connector_database_names: &std::collections::HashMap<String, Vec<String>>,
    connector_secondary_attaches: &std::collections::HashMap<String, Vec<SecondaryAttach>>,
) {
    let start_time = std::time::Instant::now();

    // Load extensions
    if let Err(e) = setup_extensions(conn, &source, &target) {
        finish_job_with_error(&mut job, &format!("Extension setup failed: {}", e), etl_jobs_mutex);
        emit_complete(app_handle, &job, 0, 0, start_time);
        persist_jobs(
            etl_jobs_mutex,
            catalog_path,
            connectors_map,
            data_sources_map,
            connector_catalogs_by_db,
            connector_database_names,
            connector_secondary_attaches,
        );
        return;
    }

    // Activate connectors on this connection
    let source_ops = get_ops(&source.connector_type);
    let target_ops = get_ops(&target.connector_type);

    if let Err(e) = source_ops.activate(conn, &source) {
        finish_job_with_error(&mut job, &format!("Source activation failed: {}", e), etl_jobs_mutex);
        emit_complete(app_handle, &job, 0, 0, start_time);
        persist_jobs(
            etl_jobs_mutex,
            catalog_path,
            connectors_map,
            data_sources_map,
            connector_catalogs_by_db,
            connector_database_names,
            connector_secondary_attaches,
        );
        return;
    }
    if let Err(e) = target_ops.activate(conn, &target) {
        finish_job_with_error(&mut job, &format!("Target activation failed: {}", e), etl_jobs_mutex);
        emit_complete(app_handle, &job, 0, 0, start_time);
        persist_jobs(
            etl_jobs_mutex,
            catalog_path,
            connectors_map,
            data_sources_map,
            connector_catalogs_by_db,
            connector_database_names,
            connector_secondary_attaches,
        );
        return;
    }

    // Introspect source
    let entries = match source_ops.introspect(conn, &source) {
        Ok(e) => e,
        Err(e) => {
            finish_job_with_error(&mut job, &format!("Source introspection failed: {}", e), etl_jobs_mutex);
            emit_complete(app_handle, &job, 0, 0, start_time);
            persist_jobs(
                etl_jobs_mutex,
                catalog_path,
                connectors_map,
                data_sources_map,
                connector_catalogs_by_db,
                connector_database_names,
                connector_secondary_attaches,
            );
            return;
        }
    };

    let tables = filter_entries(
        &entries,
        &job.include_schemas,
        &job.exclude_tables,
        job.skip_views,
    );

    if tables.is_empty() {
        job.status = JobStatus::Completed;
        etl_jobs_mutex.lock().insert(job.id.clone(), job.clone());
        emit_complete(app_handle, &job, 0, 0, start_time);
        persist_jobs(
            etl_jobs_mutex,
            catalog_path,
            connectors_map,
            data_sources_map,
            connector_catalogs_by_db,
            connector_database_names,
            connector_secondary_attaches,
        );
        return;
    }

    let pg_alias = source.alias.as_deref().unwrap_or("pg_source");
    let dl_alias = target.alias.as_deref().unwrap_or("dl_target");

    // Build previous bookmark map from existing table_states
    let mut bookmark_map: std::collections::HashMap<(String, String), TableSyncState> =
        std::collections::HashMap::new();
    for ts in &job.table_states {
        bookmark_map.insert(
            (ts.schema_name.clone(), ts.table_name.clone()),
            ts.clone(),
        );
    }

    // Create schemas in DuckLake
    let schemas: HashSet<String> = tables
        .iter()
        .filter_map(|t| t.schema.clone())
        .collect();
    for schema in &schemas {
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\".\"{}\";",
            dl_alias, schema
        );
        conn.execute_batch(&sql).ok();
    }

    // Initialize table states
    let total = tables.len();
    let mut new_states: Vec<TableSyncState> = Vec::with_capacity(total);
    for t in &tables {
        let schema_name = t.schema.clone().unwrap_or_else(|| "public".to_string());
        let prev = bookmark_map.get(&(schema_name.clone(), t.name.clone()));
        new_states.push(TableSyncState {
            schema_name,
            table_name: t.name.clone(),
            status: TableStatus::Pending,
            rows_synced: None,
            error: None,
            started_at: None,
            completed_at: None,
            replication_key: prev.and_then(|p| p.replication_key.clone()),
            replication_value: prev.and_then(|p| p.replication_value.clone()),
        });
    }
    job.table_states = new_states;

    let mut migrated = 0usize;
    let mut failed = 0usize;
    let mut total_rows: u64 = 0;

    for idx in 0..total {
        if cancel_flag.load(Ordering::SeqCst) {
            for remaining in idx..total {
                job.table_states[remaining].status = TableStatus::Skipped;
            }
            job.status = JobStatus::Cancelled;
            etl_jobs_mutex.lock().insert(job.id.clone(), job.clone());
            emit_complete(app_handle, &job, migrated, failed, start_time);
            persist_jobs(
                etl_jobs_mutex,
                catalog_path,
                connectors_map,
                data_sources_map,
                connector_catalogs_by_db,
                connector_database_names,
                connector_secondary_attaches,
            );
            return;
        }

        let schema = &job.table_states[idx].schema_name.clone();
        let table = &job.table_states[idx].table_name.clone();
        let table_start = std::time::Instant::now();

        job.table_states[idx].status = TableStatus::Running;
        job.table_states[idx].started_at = Some(chrono::Utc::now().to_rfc3339());

        let _ = app_handle.emit(
            "etl-progress",
            EtlProgressEvent {
                job_id: job.id.clone(),
                phase: "table".to_string(),
                current_table_index: idx,
                total_tables: total,
                schema_name: schema.clone(),
                table_name: table.clone(),
                status: "running".to_string(),
                rows_synced: None,
                error: None,
                elapsed_ms: start_time.elapsed().as_millis() as u64,
            },
        );

        let result = migrate_table(
            conn,
            pg_alias,
            dl_alias,
            schema,
            table,
            &job.strategy,
            &job.table_states[idx],
        );

        match result {
            Ok((rows, new_bookmark)) => {
                job.table_states[idx].status = TableStatus::Completed;
                job.table_states[idx].rows_synced = Some(rows);
                job.table_states[idx].completed_at = Some(chrono::Utc::now().to_rfc3339());
                if let Some(bm) = new_bookmark {
                    job.table_states[idx].replication_value = Some(bm);
                }
                total_rows += rows;
                migrated += 1;

                let _ = app_handle.emit(
                    "etl-progress",
                    EtlProgressEvent {
                        job_id: job.id.clone(),
                        phase: "table".to_string(),
                        current_table_index: idx,
                        total_tables: total,
                        schema_name: schema.clone(),
                        table_name: table.clone(),
                        status: "done".to_string(),
                        rows_synced: Some(rows),
                        error: None,
                        elapsed_ms: table_start.elapsed().as_millis() as u64,
                    },
                );
            }
            Err(e) => {
                let err_msg = e.to_string();
                job.table_states[idx].status = TableStatus::Failed;
                job.table_states[idx].error = Some(err_msg.clone());
                job.table_states[idx].completed_at = Some(chrono::Utc::now().to_rfc3339());
                failed += 1;

                let _ = app_handle.emit(
                    "etl-progress",
                    EtlProgressEvent {
                        job_id: job.id.clone(),
                        phase: "table".to_string(),
                        current_table_index: idx,
                        total_tables: total,
                        schema_name: schema.clone(),
                        table_name: table.clone(),
                        status: "failed".to_string(),
                        rows_synced: None,
                        error: Some(err_msg),
                        elapsed_ms: table_start.elapsed().as_millis() as u64,
                    },
                );
            }
        }

        // Persist after each table for crash recovery
        etl_jobs_mutex.lock().insert(job.id.clone(), job.clone());
        persist_jobs(
            etl_jobs_mutex,
            catalog_path,
            connectors_map,
            data_sources_map,
            connector_catalogs_by_db,
            connector_database_names,
            connector_secondary_attaches,
        );
    }

    job.total_rows_synced = total_rows;
    job.last_completed_at = Some(chrono::Utc::now().to_rfc3339());

    if failed == 0 {
        job.status = JobStatus::Completed;
    } else if migrated > 0 {
        job.status = JobStatus::Partial;
    } else {
        job.status = JobStatus::Failed;
    }

    etl_jobs_mutex.lock().insert(job.id.clone(), job.clone());
    emit_complete(app_handle, &job, migrated, failed, start_time);
    persist_jobs(
        etl_jobs_mutex,
        catalog_path,
        connectors_map,
        data_sources_map,
        connector_catalogs_by_db,
        connector_database_names,
        connector_secondary_attaches,
    );

    // Cleanup: detach the cloned connection's attachments
    source_ops.deactivate(conn, &source).ok();
    target_ops.deactivate(conn, &target).ok();
}

// ──────────────────────────── Helpers ────────────────────────────

fn setup_extensions(
    conn: &Connection,
    source: &crate::state::Connector,
    target: &crate::state::Connector,
) -> Result<(), AppError> {
    ensure_extension(conn, "postgres")?;
    ensure_extension(conn, "ducklake")?;
    ensure_extension(conn, "httpfs")?;

    let catalog_type = target.config.catalog_type.as_deref().unwrap_or("duckdb");
    match catalog_type {
        "postgres" => { ensure_extension(conn, "postgres")?; }
        "sqlite" => { ensure_extension(conn, "sqlite")?; }
        _ => {}
    }

    let _ = source;
    Ok(())
}

fn filter_entries(
    entries: &[CatalogEntry],
    include_schemas: &Option<Vec<String>>,
    exclude_tables: &Option<Vec<String>>,
    skip_views: bool,
) -> Vec<CatalogEntry> {
    let exclude_set: HashSet<String> = exclude_tables
        .as_ref()
        .map(|v| v.iter().cloned().collect())
        .unwrap_or_default();

    entries
        .iter()
        .filter(|e| {
            if skip_views && e.entry_type == "view" {
                return false;
            }
            if let Some(schemas) = include_schemas {
                if let Some(s) = &e.schema {
                    if !schemas.contains(s) {
                        return false;
                    }
                }
            }
            let qualified = format!(
                "{}.{}",
                e.schema.as_deref().unwrap_or("public"),
                e.name
            );
            !exclude_set.contains(&qualified) && !exclude_set.contains(&e.name)
        })
        .cloned()
        .collect()
}

fn migrate_table(
    conn: &Connection,
    pg_alias: &str,
    dl_alias: &str,
    schema: &str,
    table: &str,
    strategy: &SyncStrategy,
    prev_state: &TableSyncState,
) -> Result<(u64, Option<String>), AppError> {
    let pg_ref = format!(
        "\"{}\".\"{}\".\"{}\"",
        escape_sql_string(pg_alias),
        escape_sql_string(schema),
        escape_sql_string(table)
    );
    let dl_ref = format!(
        "\"{}\".\"{}\".\"{}\"",
        escape_sql_string(dl_alias),
        escape_sql_string(schema),
        escape_sql_string(table)
    );

    match strategy {
        SyncStrategy::Full => {
            conn.execute_batch(&format!("DROP TABLE IF EXISTS {};", dl_ref))
                .map_err(|e| AppError::EtlError(format!("Drop table {}: {}", table, e)))?;

            conn.execute_batch(&format!(
                "CREATE TABLE {} AS SELECT * FROM {};",
                dl_ref, pg_ref
            ))
            .map_err(|e| AppError::EtlError(format!("Create table {}: {}", table, e)))?;

            let rows = count_table(conn, &dl_ref);
            Ok((rows, None))
        }
        SyncStrategy::Incremental => {
            // Create table if it doesn't exist (empty schema copy)
            let exists = table_exists_in_ducklake(conn, dl_alias, schema, table);
            if !exists {
                conn.execute_batch(&format!(
                    "CREATE TABLE {} AS SELECT * FROM {} WHERE 1=0;",
                    dl_ref, pg_ref
                ))
                .map_err(|e| {
                    AppError::EtlError(format!("Create empty table {}: {}", table, e))
                })?;
            }

            let replication_key = prev_state.replication_key.as_deref();
            let last_value = prev_state.replication_value.as_deref();

            let insert_sql = match (replication_key, last_value) {
                (Some(key), Some(val)) => {
                    format!(
                        "INSERT INTO {} SELECT * FROM {} WHERE \"{}\" > '{}';",
                        dl_ref,
                        pg_ref,
                        escape_sql_string(key),
                        escape_sql_string(val)
                    )
                }
                _ => {
                    // No bookmark yet — do a full initial load into the empty table
                    format!(
                        "INSERT INTO {} SELECT * FROM {};",
                        dl_ref, pg_ref
                    )
                }
            };

            conn.execute_batch(&insert_sql)
                .map_err(|e| AppError::EtlError(format!("Insert into {}: {}", table, e)))?;

            let rows = count_table(conn, &dl_ref);

            // Update bookmark
            let new_bookmark = if let Some(key) = replication_key {
                let bm_sql = format!(
                    "SELECT MAX(\"{}\")::VARCHAR FROM {}",
                    escape_sql_string(key),
                    pg_ref
                );
                conn.query_row(&bm_sql, params![], |row| row.get::<_, String>(0))
                    .ok()
            } else {
                None
            };

            Ok((rows, new_bookmark))
        }
        SyncStrategy::Append => {
            let exists = table_exists_in_ducklake(conn, dl_alias, schema, table);
            if !exists {
                conn.execute_batch(&format!(
                    "CREATE TABLE {} AS SELECT * FROM {};",
                    dl_ref, pg_ref
                ))
                .map_err(|e| {
                    AppError::EtlError(format!("Create table {}: {}", table, e))
                })?;
            } else {
                conn.execute_batch(&format!(
                    "INSERT INTO {} SELECT * FROM {};",
                    dl_ref, pg_ref
                ))
                .map_err(|e| AppError::EtlError(format!("Append to {}: {}", table, e)))?;
            }
            let rows = count_table(conn, &dl_ref);
            Ok((rows, None))
        }
    }
}

fn table_exists_in_ducklake(
    conn: &Connection,
    dl_alias: &str,
    schema: &str,
    table: &str,
) -> bool {
    let sql = format!(
        "SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = '{}' AND schema_name = '{}' AND table_name = '{}'",
        escape_sql_string(dl_alias),
        escape_sql_string(schema),
        escape_sql_string(table),
    );
    conn.query_row(&sql, params![], |row| row.get::<_, i64>(0))
        .map(|c| c > 0)
        .unwrap_or(false)
}

fn count_table(conn: &Connection, qualified_ref: &str) -> u64 {
    let sql = format!("SELECT COUNT(*) FROM {}", qualified_ref);
    conn.query_row(&sql, params![], |row| row.get::<_, u64>(0))
        .unwrap_or(0)
}

fn finish_job_with_error(
    job: &mut EtlJob,
    error: &str,
    etl_jobs_mutex: &parking_lot::Mutex<std::collections::HashMap<String, EtlJob>>,
) {
    eprintln!("ETL job {} error: {}", job.id, error);
    job.status = JobStatus::Failed;
    job.last_completed_at = Some(chrono::Utc::now().to_rfc3339());
    etl_jobs_mutex.lock().insert(job.id.clone(), job.clone());
}

fn emit_complete(
    app_handle: &tauri::AppHandle,
    job: &EtlJob,
    migrated: usize,
    failed: usize,
    start_time: std::time::Instant,
) {
    let status_str = match job.status {
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
        JobStatus::Cancelled => "cancelled",
        JobStatus::Partial => "partial",
        _ => "completed",
    };
    let _ = app_handle.emit(
        "etl-complete",
        EtlCompleteEvent {
            job_id: job.id.clone(),
            status: status_str.to_string(),
            tables_migrated: migrated,
            tables_failed: failed,
            total_rows: job.total_rows_synced,
            elapsed_ms: start_time.elapsed().as_millis() as u64,
        },
    );
}

fn persist_jobs(
    etl_jobs_mutex: &parking_lot::Mutex<std::collections::HashMap<String, EtlJob>>,
    catalog_path: &std::path::Path,
    connectors_map: &std::collections::HashMap<String, crate::state::Connector>,
    data_sources_map: &std::collections::HashMap<String, crate::state::DataSource>,
    connector_catalogs_by_db: &std::collections::HashMap<String, CatalogByDatabase>,
    connector_database_names: &std::collections::HashMap<String, Vec<String>>,
    connector_secondary_attaches: &std::collections::HashMap<String, Vec<SecondaryAttach>>,
) {
    let jobs = etl_jobs_mutex.lock().clone();
    save_catalog_direct(
        catalog_path,
        connectors_map,
        data_sources_map,
        connector_catalogs_by_db,
        connector_database_names,
        connector_secondary_attaches,
        &jobs,
    );
}
