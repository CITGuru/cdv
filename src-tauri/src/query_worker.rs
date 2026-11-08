use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde::{Deserialize, Serialize};

use crate::error::AppError;

// ─── Wire protocol ─────────────────────────────────────────────────
//
// Messages are length-prefixed:
//   [4 bytes LE: payload length] [payload bytes]
//
// Requests  (parent → child):  payload = JSON
// Responses (child  → parent): [1 byte status: 0=ok | 1=err] [data]

fn write_msg(w: &mut impl Write, data: &[u8]) -> std::io::Result<()> {
    w.write_all(&(data.len() as u32).to_le_bytes())?;
    w.write_all(data)?;
    w.flush()
}

fn read_msg(r: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut hdr = [0u8; 4];
    r.read_exact(&mut hdr)?;
    let n = u32::from_le_bytes(hdr) as usize;
    let mut buf = vec![0u8; n];
    if n > 0 {
        r.read_exact(&mut buf)?;
    }
    Ok(buf)
}

fn write_ok(w: &mut impl Write, data: &[u8]) -> std::io::Result<()> {
    let total = 1 + data.len();
    w.write_all(&(total as u32).to_le_bytes())?;
    w.write_all(&[0u8])?;
    w.write_all(data)?;
    w.flush()
}

fn write_err_response(w: &mut impl Write, msg: &str) -> std::io::Result<()> {
    let bytes = msg.as_bytes();
    let total = 1 + bytes.len();
    w.write_all(&(total as u32).to_le_bytes())?;
    w.write_all(&[1u8])?;
    w.write_all(bytes)?;
    w.flush()
}

fn read_response(r: &mut impl Read) -> std::io::Result<Result<Vec<u8>, String>> {
    let mut hdr = [0u8; 4];
    r.read_exact(&mut hdr)?;
    let total = u32::from_le_bytes(hdr) as usize;
    if total == 0 {
        return Ok(Ok(Vec::new()));
    }
    let mut buf = vec![0u8; total];
    r.read_exact(&mut buf)?;
    if buf[0] == 0 {
        Ok(Ok(buf[1..].to_vec()))
    } else {
        Ok(Err(String::from_utf8_lossy(&buf[1..]).into_owned()))
    }
}

// ─── Request types ─────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum Request {
    #[serde(rename = "init")]
    Init {
        db_path: String,
        catalog_path: String,
    },
    #[serde(rename = "query")]
    Query { sql: String },
}

// ─── Parent-side: manages the worker subprocess ────────────────────

pub struct QueryWorker {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    db_path: PathBuf,
    catalog_path: PathBuf,
    /// Views changed since this worker started (data source or directory
    /// change). The next query restarts the worker first.
    stale: bool,
}

impl QueryWorker {
    pub fn spawn(db_path: PathBuf, catalog_path: PathBuf) -> Result<Self, AppError> {
        let exe = std::env::current_exe()
            .map_err(|e| AppError::QueryError(format!("Cannot find own executable: {e}")))?;

        let mut child = Command::new(exe)
            .arg("--query-worker")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| AppError::QueryError(format!("Failed to spawn query worker: {e}")))?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let mut w = QueryWorker {
            child,
            stdin,
            stdout,
            db_path: db_path.clone(),
            catalog_path: catalog_path.clone(),
            stale: false,
        };

        let req = Request::Init {
            db_path: db_path.to_string_lossy().into_owned(),
            catalog_path: catalog_path.to_string_lossy().into_owned(),
        };
        let json = serde_json::to_vec(&req).unwrap();
        write_msg(&mut w.stdin, &json)
            .map_err(|e| AppError::QueryError(format!("Worker init write failed: {e}")))?;

        match read_response(&mut w.stdout) {
            Ok(Ok(_)) => {
                eprintln!("[query-worker] Started (pid={})", w.child.id());
                Ok(w)
            }
            Ok(Err(msg)) => Err(AppError::QueryError(format!("Worker init failed: {msg}"))),
            Err(e) => Err(AppError::QueryError(format!("Worker init read failed: {e}"))),
        }
    }

    /// Execute a SELECT query in the isolated worker process.
    /// Returns Arrow IPC bytes on success.
    pub fn execute(&mut self, sql: &str) -> Result<Vec<u8>, AppError> {
        if self.stale {
            eprintln!("[query-worker] Views changed, restarting worker before query");
            self.respawn()?;
        }
        let req = Request::Query {
            sql: sql.to_owned(),
        };
        let json = serde_json::to_vec(&req).unwrap();

        write_msg(&mut self.stdin, &json).map_err(|e| {
            AppError::QueryError(format!(
                "Query worker process crashed (write failed): {e}"
            ))
        })?;

        match read_response(&mut self.stdout) {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(msg)) => Err(AppError::QueryError(msg)),
            Err(e) => Err(AppError::QueryError(format!(
                "Query worker process crashed (read failed): {e}"
            ))),
        }
    }

    #[allow(dead_code)]
    pub fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Kill the current worker and spawn a fresh one.
    pub fn respawn(&mut self) -> Result<(), AppError> {
        self.child.kill().ok();
        self.child.wait().ok();
        let mut fresh = Self::spawn(self.db_path.clone(), self.catalog_path.clone())?;
        std::mem::swap(self, &mut fresh);
        drop(fresh);
        Ok(())
    }
}

/// Mark the worker stale after a change that adds, removes, or renames
/// views. Call it after the catalog is saved. The next query restarts the
/// worker, which rebuilds its views from the saved catalog and manifests, so
/// many changes in a row (for example a multi-table import) cost one restart.
pub fn reload(state: &crate::state::AppState) {
    if let Some(w) = state.query_worker.lock().as_mut() {
        w.stale = true;
    }
}

impl Drop for QueryWorker {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

// ─── Child-side: entry point for the worker subprocess ─────────────

pub fn worker_main() {
    let mut stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let init_data = match read_msg(&mut stdin) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("[query-worker] stdin read failed: {e}");
            std::process::exit(1);
        }
    };

    let req: Request = match serde_json::from_slice(&init_data) {
        Ok(r) => r,
        Err(e) => {
            let _ = write_err_response(&mut stdout, &format!("bad init json: {e}"));
            std::process::exit(1);
        }
    };

    let (db_path, catalog_path) = match req {
        Request::Init {
            db_path,
            catalog_path,
        } => (db_path, catalog_path),
        _ => {
            let _ = write_err_response(&mut stdout, "expected init message");
            std::process::exit(1);
        }
    };

    let conn = match init_connection(&db_path, &catalog_path) {
        Ok(c) => c,
        Err(msg) => {
            let _ = write_err_response(&mut stdout, &msg);
            std::process::exit(1);
        }
    };

    let _ = write_ok(&mut stdout, b"");

    loop {
        let data = match read_msg(&mut stdin) {
            Ok(d) => d,
            Err(_) => break,
        };

        let req: Request = match serde_json::from_slice(&data) {
            Ok(r) => r,
            Err(e) => {
                let _ = write_err_response(&mut stdout, &format!("bad json: {e}"));
                continue;
            }
        };

        match req {
            Request::Query { sql } => {
                eprintln!(
                    "[query-worker] exec: {}",
                    if sql.len() > 200 { &sql[..200] } else { &sql }
                );
                match exec_query(&conn, &sql) {
                    Ok(ipc) => {
                        let _ = write_ok(&mut stdout, &ipc);
                    }
                    Err(msg) => {
                        let _ = write_err_response(&mut stdout, &msg);
                    }
                }
            }
            _ => {
                let _ = write_err_response(&mut stdout, "unexpected message type");
            }
        }
    }
}

fn init_connection(db_path: &str, catalog_path: &str) -> Result<duckdb::Connection, String> {
    use duckdb::Connection;

    eprintln!("[query-worker] init db={db_path} catalog={catalog_path}");

    let conn = Connection::open_in_memory().map_err(|e| format!("open_in_memory: {e}"))?;

    // Single-threaded execution reduces the likelihood of hitting
    // multi-threaded compression bugs (the StringCompress crash).
    conn.execute_batch("SET threads TO 1").ok();

    conn.execute_batch("INSTALL excel; LOAD excel;").ok();

    // Attach the persistent database read-only so we can see its
    // tables, views, and persistent secrets.
    let has_main_db = if std::path::Path::new(db_path).exists() {
        let sql = format!(
            "ATTACH '{}' AS cdv_db (READ_ONLY)",
            db_path.replace('\'', "''")
        );
        match conn.execute_batch(&sql) {
            Ok(()) => {
                eprintln!("[query-worker] Attached main DB as read-only");
                true
            }
            Err(e) => {
                eprintln!("[query-worker] ATTACH main db failed (non-fatal): {e}");
                false
            }
        }
    } else {
        false
    };

    let cat = crate::catalog::load_catalog(std::path::Path::new(catalog_path));

    for c in &cat.connectors {
        let ops = crate::connector::get_ops(&c.connector_type);
        if let Err(e) = ops.activate(&conn, c) {
            eprintln!(
                "[query-worker] connector '{}' activate failed (non-fatal): {}",
                c.name, e
            );
        }
    }

    crate::connector::rehydrate_secondary_attaches(
        &conn,
        &cat.connector_secondary_attaches,
        &cat.connectors,
    );

    let cmap: std::collections::HashMap<String, crate::state::Connector> = cat
        .connectors
        .iter()
        .map(|c| (c.id.clone(), c.clone()))
        .collect();
    if let Err(e) = crate::dataset_manager::rehydrate_views(&conn, &cat.data_sources, &cmap) {
        eprintln!("[query-worker] rehydrate_views warning: {e}");
    }

    // Proxy views for tables that live inside the persistent DuckDB file.
    // The worker's default database is `memory`, so unqualified table
    // references need a shim pointing at `cdv_db.<qualified_name>`.
    if has_main_db {
        for ds in &cat.data_sources {
            if ds.kind != "table" {
                continue;
            }
            let proxy = format!(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM cdv_db.{}",
                ds.qualified_name, ds.qualified_name
            );
            if let Err(e) = conn.execute_batch(&proxy) {
                eprintln!(
                    "[query-worker] proxy view for '{}' failed (non-fatal): {e}",
                    ds.name
                );
            }
        }
    }

    eprintln!("[query-worker] init complete");
    Ok(conn)
}

fn exec_query(conn: &duckdb::Connection, sql: &str) -> Result<Vec<u8>, String> {
    use duckdb::arrow::record_batch::RecordBatch;
    use duckdb::params;

    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let frames = stmt.query_arrow(params![]).map_err(|e| e.to_string())?;
    let batches: Vec<RecordBatch> = frames.collect();

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    crate::query_engine::batches_to_ipc(&batches[0].schema(), &batches).map_err(|e| e.to_string())
}
