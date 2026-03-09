use duckdb::params;
use tauri::State;

use crate::catalog;
use crate::error::AppError;
use crate::state::{AppState, Connector, ConnectorConfig, ConnectorType};

#[tauri::command]
pub fn create_connection(
    name: String,
    provider: String,
    endpoint: Option<String>,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    prefix: Option<String>,
    account_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<Connector, AppError> {
    let ct = match provider.as_str() {
        "gcp" => ConnectorType::GCS,
        "cloudflare" => ConnectorType::R2,
        _ => ConnectorType::S3,
    };

    let id = uuid::Uuid::new_v4().to_string();
    let secret_name = format!("cdv_{}", id.replace('-', "_"));

    let connector = Connector {
        id: id.clone(),
        name,
        connector_type: ct,
        config: ConnectorConfig {
            bucket: Some(bucket),
            region: Some(region),
            endpoint,
            prefix,
            account_id,
            user: Some(access_key.clone()),
            password: Some(secret_key.clone()),
            ..Default::default()
        },
        alias: None,
        secret_name: Some(secret_name.clone()),
    };

    let ops = crate::connector::get_ops(&connector.connector_type);
    let conn = state.conn.lock();
    ops.activate(&conn, &connector)?;
    drop(conn);

    state
        .connectors
        .lock()
        .insert(id.clone(), connector.clone());
    catalog::save_state_catalog(&state);

    Ok(connector)
}

#[tauri::command]
pub fn remove_connection(id: String, state: State<'_, AppState>) -> Result<(), AppError> {
    let connector = {
        let mut connectors = state.connectors.lock();
        connectors
            .remove(&id)
            .ok_or_else(|| AppError::AuthError("Connection not found".to_string()))?
    };

    let ops = crate::connector::get_ops(&connector.connector_type);
    let conn = state.conn.lock();
    ops.deactivate(&conn, &connector).ok();
    drop(conn);

    catalog::save_state_catalog(&state);

    Ok(())
}

#[tauri::command]
pub fn list_connections(state: State<'_, AppState>) -> Result<Vec<Connector>, AppError> {
    let connectors = state.connectors.lock();
    let cloud: Vec<Connector> = connectors
        .values()
        .filter(|c| {
            matches!(
                c.connector_type,
                ConnectorType::S3 | ConnectorType::GCS | ConnectorType::R2
            )
        })
        .cloned()
        .collect();
    Ok(cloud)
}

#[tauri::command]
pub fn list_connection_files(
    connection_id: String,
    prefix_override: Option<String>,
    state: State<'_, AppState>,
) -> Result<Vec<String>, AppError> {
    let connector = {
        let connectors = state.connectors.lock();
        connectors
            .get(&connection_id)
            .cloned()
            .ok_or_else(|| AppError::AuthError("Connection not found".to_string()))?
    };

    let cfg = &connector.config;
    let bucket = cfg
        .bucket
        .as_deref()
        .ok_or_else(|| AppError::AuthError("Missing bucket".into()))?;

    let scheme = match connector.connector_type {
        ConnectorType::GCS => "gcs",
        _ => "s3",
    };
    let prefix = prefix_override.or_else(|| cfg.prefix.clone());
    let path = match prefix {
        Some(p) if !p.is_empty() => format!("{}://{}/{}*", scheme, bucket, p),
        _ => format!("{}://{}/*", scheme, bucket),
    };

    let conn = state.conn.lock();
    let sql = format!("SELECT file FROM glob('{}')", path);
    let mut stmt = conn.prepare(&sql)?;
    let files: Vec<String> = stmt
        .query_map(params![], |row| row.get(0))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(files)
}
