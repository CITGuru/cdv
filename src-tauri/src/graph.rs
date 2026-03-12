use duckdb::params;
use tauri::State;

use crate::error::AppError;
use crate::query_engine::batches_to_ipc;
use crate::state::{AppState, EdgeTableDef, PropertyGraphInfo, VertexTableDef};

fn require_graph(state: &AppState) -> Result<(), AppError> {
    if !*state.graph_enabled.lock() {
        return Err(AppError::GraphError(
            "DuckPGQ extension not available".into(),
        ));
    }
    Ok(())
}

fn quote_ident(id: &str) -> String {
    if id.contains('.') || id.contains('"') {
        id.to_string()
    } else {
        format!("\"{}\"", id)
    }
}

fn build_create_ddl(
    name: &str,
    vertex_tables: &[VertexTableDef],
    edge_tables: &[EdgeTableDef],
) -> String {
    let mut ddl = format!("CREATE PROPERTY GRAPH {}", quote_ident(name));

    ddl.push_str("\nVERTEX TABLES (\n");
    for (i, vt) in vertex_tables.iter().enumerate() {
        if i > 0 {
            ddl.push_str(",\n");
        }
        ddl.push_str(&format!("    {}", quote_ident(&vt.table_name)));
        if let Some(label) = &vt.label {
            ddl.push_str(&format!(" LABEL {}", quote_ident(label)));
        }
    }
    ddl.push_str("\n)");

    if !edge_tables.is_empty() {
        ddl.push_str("\nEDGE TABLES (\n");
        for (i, et) in edge_tables.iter().enumerate() {
            if i > 0 {
                ddl.push_str(",\n");
            }
            ddl.push_str(&format!(
                "    {}\n        SOURCE KEY ({}) REFERENCES {} ({})\n        DESTINATION KEY ({}) REFERENCES {} ({})",
                quote_ident(&et.table_name),
                quote_ident(&et.source_key),
                quote_ident(&et.source_vertex_table),
                quote_ident(&et.source_vertex_key),
                quote_ident(&et.destination_key),
                quote_ident(&et.destination_vertex_table),
                quote_ident(&et.destination_vertex_key),
            ));
            if let Some(label) = &et.label {
                ddl.push_str(&format!(" LABEL {}", quote_ident(label)));
            }
        }
        ddl.push_str("\n)");
    }

    ddl.push(';');
    ddl
}

#[tauri::command]
pub fn check_graph_support(state: State<'_, AppState>) -> bool {
    *state.graph_enabled.lock()
}

#[tauri::command]
pub fn install_graph_extension(state: State<'_, AppState>) -> Result<(), AppError> {
    let conn = state.conn.lock();
    conn.execute_batch("INSTALL duckpgq FROM community; LOAD duckpgq;")
        .map_err(|e| AppError::GraphError(format!(
            "Failed to install DuckPGQ extension: {}. Ensure you have an active internet connection.",
            e
        )))?;
    *state.graph_enabled.lock() = true;
    Ok(())
}

#[tauri::command]
pub fn create_property_graph(
    name: String,
    vertex_tables: Vec<VertexTableDef>,
    edge_tables: Vec<EdgeTableDef>,
    state: State<'_, AppState>,
) -> Result<(), AppError> {
    require_graph(&state)?;
    if vertex_tables.is_empty() {
        return Err(AppError::GraphError(
            "At least one vertex table is required".into(),
        ));
    }
    let ddl = build_create_ddl(&name, &vertex_tables, &edge_tables);
    let conn = state.conn.lock();
    conn.execute_batch(&ddl)
        .map_err(|e| AppError::GraphError(format!("Failed to create property graph: {}", e)))?;
    Ok(())
}

#[tauri::command]
pub fn list_property_graphs(state: State<'_, AppState>) -> Result<Vec<PropertyGraphInfo>, AppError> {
    require_graph(&state)?;
    let conn = state.conn.lock();
    let mut stmt = conn
        .prepare("SELECT property_graph, table_name, is_vertex_table FROM duckpgq_tables()")
        .map_err(|e| AppError::GraphError(e.to_string()))?;

    let rows: Vec<(String, String, bool)> = stmt
        .query_map(params![], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|e| AppError::GraphError(e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();

    let mut graphs: std::collections::HashMap<String, PropertyGraphInfo> =
        std::collections::HashMap::new();
    for (pg_name, table_name, is_vertex) in rows {
        let info = graphs.entry(pg_name.clone()).or_insert_with(|| PropertyGraphInfo {
            name: pg_name,
            vertex_tables: Vec::new(),
            edge_tables: Vec::new(),
        });
        if is_vertex {
            info.vertex_tables.push(table_name);
        } else {
            info.edge_tables.push(table_name);
        }
    }

    Ok(graphs.into_values().collect())
}

#[tauri::command]
pub fn drop_property_graph(name: String, state: State<'_, AppState>) -> Result<(), AppError> {
    require_graph(&state)?;
    let conn = state.conn.lock();
    conn.execute_batch(&format!("DROP PROPERTY GRAPH \"{}\"", name))
        .map_err(|e| AppError::GraphError(format!("Failed to drop property graph: {}", e)))?;
    Ok(())
}

#[tauri::command]
pub fn get_property_graph_info(
    name: String,
    state: State<'_, AppState>,
) -> Result<PropertyGraphInfo, AppError> {
    require_graph(&state)?;
    let conn = state.conn.lock();
    let mut stmt = conn
        .prepare(
            "SELECT table_name, is_vertex_table FROM duckpgq_tables() WHERE property_graph = ?",
        )
        .map_err(|e| AppError::GraphError(e.to_string()))?;

    let rows: Vec<(String, bool)> = stmt
        .query_map(params![name], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|e| AppError::GraphError(e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();

    if rows.is_empty() {
        return Err(AppError::GraphError(format!(
            "Property graph '{}' not found",
            name
        )));
    }

    let mut info = PropertyGraphInfo {
        name,
        vertex_tables: Vec::new(),
        edge_tables: Vec::new(),
    };
    for (table_name, is_vertex) in rows {
        if is_vertex {
            info.vertex_tables.push(table_name);
        } else {
            info.edge_tables.push(table_name);
        }
    }
    Ok(info)
}

#[tauri::command]
pub fn run_graph_algorithm(
    graph_name: String,
    algorithm: String,
    vertex_label: String,
    edge_label: String,
    state: State<'_, AppState>,
) -> Result<Vec<u8>, AppError> {
    require_graph(&state)?;

    let sql = match algorithm.as_str() {
        "pagerank" => format!(
            "FROM pagerank(\"{}\", \"{}\", \"{}\")",
            graph_name, vertex_label, edge_label
        ),
        "local_clustering_coefficient" => format!(
            "FROM local_clustering_coefficient(\"{}\", \"{}\", \"{}\")",
            graph_name, vertex_label, edge_label
        ),
        "weakly_connected_component" => format!(
            "FROM weakly_connected_component(\"{}\", \"{}\", \"{}\")",
            graph_name, vertex_label, edge_label
        ),
        other => {
            return Err(AppError::GraphError(format!(
                "Unknown algorithm: {}",
                other
            )));
        }
    };

    let batches: Vec<duckdb::arrow::record_batch::RecordBatch> = {
        let conn = state.conn.lock();
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| AppError::GraphError(e.to_string()))?;
        let frames = stmt
            .query_arrow(params![])
            .map_err(|e| AppError::GraphError(e.to_string()))?;
        frames.collect()
    };

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    batches_to_ipc(&batches[0].schema(), &batches)
}
