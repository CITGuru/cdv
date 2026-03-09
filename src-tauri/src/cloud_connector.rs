use duckdb::params;
use tauri::State;

use crate::catalog;
use crate::error::AppError;
use crate::state::{AppState, ConnectionInfo};

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
) -> Result<ConnectionInfo, AppError> {
    let conn = state.conn.lock();

    conn.execute_batch("INSTALL httpfs; LOAD httpfs;")
        .map_err(|e| AppError::AuthError(format!("Failed to load HTTPFS: {}", e)))?;

    let id = uuid::Uuid::new_v4().to_string();
    let secret_name = format!("cdv_{}", id.replace("-", "_"));

    let parts: Vec<String> = match provider.as_str() {
        "gcp" => {
            let scope = format!("gcs://{}", bucket);
            vec![
                "TYPE GCS".to_string(),
                format!("KEY_ID '{}'", access_key),
                format!("SECRET '{}'", secret_key),
                format!("SCOPE '{}'", scope),
            ]
        }
        "cloudflare" => {
            let account = account_id.as_deref().filter(|s| !s.is_empty()).ok_or_else(|| {
                AppError::AuthError("Cloudflare R2 requires Account ID".to_string())
            })?;
            vec![
                "TYPE R2".to_string(),
                format!("ACCOUNT_ID '{}'", account),
                format!("KEY_ID '{}'", access_key),
                format!("SECRET '{}'", secret_key),
                format!("REGION '{}'", if region.is_empty() { "auto" } else { region.as_str() }),
            ]
        }
        _ => {
            // s3 or s3-compatible
            let scope = format!("s3://{}", bucket);
            let mut parts = vec![
                "TYPE S3".to_string(),
                format!("KEY_ID '{}'", access_key),
                format!("SECRET '{}'", secret_key),
                format!("REGION '{}'", if region.is_empty() { "us-east-1" } else { region.as_str() }),
                format!("SCOPE '{}'", scope),
            ];
            if let Some(ep) = &endpoint {
                if !ep.is_empty() {
                    parts.push(format!("ENDPOINT '{}'", ep));
                    parts.push("URL_STYLE 'path'".to_string());
                }
            }
            parts
        }
    };

    let secret_sql = format!(
        "CREATE SECRET \"{}\" ({})",
        secret_name,
        parts.join(", ")
    );

    conn.execute_batch(&secret_sql)
        .map_err(|e| AppError::AuthError(format!("Failed to create secret: {}", e)))?;

    let connection = ConnectionInfo {
        id: id.clone(),
        name,
        provider: provider.to_lowercase(),
        endpoint,
        bucket,
        region,
        prefix,
        account_id,
        secret_name,
    };

    drop(conn);
    state.connections.lock().insert(id.clone(), connection.clone());

    let catalog = catalog::catalog_from_state(
        &*state.data_sources.lock(),
        &*state.connections.lock(),
    );
    catalog::save_catalog(&state.catalog_path, &catalog).ok();

    Ok(connection)
}

#[tauri::command]
pub fn remove_connection(id: String, state: State<'_, AppState>) -> Result<(), AppError> {
    let secret_name = {
        let mut connections = state.connections.lock();
        let info = connections
            .remove(&id)
            .ok_or_else(|| AppError::AuthError("Connection not found".to_string()))?;
        info.secret_name
    };

    let conn = state.conn.lock();
    conn.execute_batch(&format!("DROP SECRET IF EXISTS \"{}\"", secret_name))
        .map_err(|e| AppError::AuthError(e.to_string()))?;
    drop(conn);

    let catalog = catalog::catalog_from_state(
        &*state.data_sources.lock(),
        &*state.connections.lock(),
    );
    catalog::save_catalog(&state.catalog_path, &catalog).ok();

    Ok(())
}

#[tauri::command]
pub fn list_connections(state: State<'_, AppState>) -> Result<Vec<ConnectionInfo>, AppError> {
    Ok(state.connections.lock().values().cloned().collect())
}

fn connection_path_scheme(provider: &str) -> &'static str {
    if provider == "gcp" {
        "gcs"
    } else {
        "s3" // s3 and cloudflare (R2) use s3:// with their respective secrets
    }
}

#[tauri::command]
pub fn list_connection_files(
    connection_id: String,
    prefix_override: Option<String>,
    state: State<'_, AppState>,
) -> Result<Vec<String>, AppError> {
    let (bucket, default_prefix, provider) = {
        let connections = state.connections.lock();
        let info = connections
            .get(&connection_id)
            .ok_or_else(|| AppError::AuthError("Connection not found".to_string()))?;
        (
            info.bucket.clone(),
            info.prefix.clone(),
            info.provider.clone(),
        )
    };

    let scheme = connection_path_scheme(&provider);
    let prefix = prefix_override.or(default_prefix);
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
