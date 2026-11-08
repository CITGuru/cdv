use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, MutexGuard};

use serde::{Deserialize, Serialize};

use crate::error::AppError;

// Wire protocol: [u32 LE length][payload].
// Requests are JSON; responses are [status: 0 ok | 1 err][data].

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
    let mut status = [0u8; 1];
    r.read_exact(&mut status)?;
    let mut buf = vec![0u8; total - 1];
    r.read_exact(&mut buf)?;
    if status[0] == 0 {
        Ok(Ok(buf))
    } else {
        Ok(Err(String::from_utf8_lossy(&buf).into_owned()))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum Request {
    #[serde(rename = "init")]
    Init { catalog_path: String, threads: u32 },
    #[serde(rename = "query")]
    Query { sql: String },
}

pub struct QueryWorker {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    catalog_path: PathBuf,
    threads: u32,
    /// Pool generation at the time this worker read the catalog.
    generation: u64,
}

impl QueryWorker {
    fn spawn(catalog_path: PathBuf, threads: u32, generation: u64) -> Result<Self, AppError> {
        let exe = worker_exe()?;

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
            catalog_path: catalog_path.clone(),
            threads,
            generation,
        };

        let req = Request::Init {
            catalog_path: catalog_path.to_string_lossy().into_owned(),
            threads,
        };
        let json = serde_json::to_vec(&req).unwrap();
        write_msg(&mut w.stdin, &json)
            .map_err(|e| AppError::QueryError(format!("Worker init write failed: {e}")))?;

        match read_response(&mut w.stdout) {
            Ok(Ok(_)) => {
                eprintln!("[query-worker] started pid={} threads={threads}", w.child.id());
                Ok(w)
            }
            Ok(Err(msg)) => Err(AppError::QueryError(format!("Worker init failed: {msg}"))),
            Err(e) => Err(AppError::QueryError(format!("Worker init read failed: {e}"))),
        }
    }

    fn execute(&mut self, sql: &str) -> Result<Vec<u8>, AppError> {
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

    fn respawn(&mut self, threads: u32, generation: u64) -> Result<(), AppError> {
        self.child.kill().ok();
        self.child.wait().ok();
        *self = Self::spawn(self.catalog_path.clone(), threads, generation)?;
        Ok(())
    }
}

fn worker_exe() -> Result<PathBuf, AppError> {
    // The test binary has no `--query-worker` entry point.
    #[cfg(test)]
    if let Some(p) = std::env::var_os("CDV_WORKER_EXE") {
        return Ok(PathBuf::from(p));
    }
    std::env::current_exe()
        .map_err(|e| AppError::QueryError(format!("Cannot find own executable: {e}")))
}

pub const POOL_SIZE: usize = 2;

const CRASH_MESSAGE: &str = "Query execution failed: the query worker process crashed due to a \
     DuckDB internal error (assertion failure in string compression), also when \
     run again with a single thread. This is a known DuckDB engine bug \
     triggered by certain data. The worker has been restarted for subsequent queries.";

fn is_crash(e: &AppError) -> bool {
    matches!(e, AppError::QueryError(msg) if msg.contains("crashed"))
}

pub struct WorkerPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    slots: Vec<Mutex<Option<QueryWorker>>>,
    next: AtomicUsize,
    generation: AtomicU64,
    catalog_path: Mutex<Option<PathBuf>>,
    threads: u32,
    refreshing: AtomicBool,
}

/// Groups a burst of changes (e.g. a multi-table import) into one refresh.
const REFRESH_DEBOUNCE: Duration = Duration::from_millis(300);

impl WorkerPool {
    pub fn new(size: usize) -> Self {
        let size = size.max(1);
        let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
        WorkerPool {
            inner: Arc::new(PoolInner {
                slots: (0..size).map(|_| Mutex::new(None)).collect(),
                next: AtomicUsize::new(0),
                generation: AtomicU64::new(0),
                catalog_path: Mutex::new(None),
                threads: (cores / size).max(1) as u32,
                refreshing: AtomicBool::new(false),
            }),
        }
    }

    /// Blocks until all workers have started (or failed to).
    pub fn start(&self, catalog_path: PathBuf) {
        *self.inner.catalog_path.lock() = Some(catalog_path);
        let generation = self.inner.generation.load(Ordering::SeqCst);
        self.inner.replace_all(generation);
        if self.inner.generation.load(Ordering::SeqCst) != generation {
            self.mark_stale();
        }
    }

    /// Replace the workers in the background. Old workers keep serving until
    /// then; tables they lack fall back to the in-process connection.
    pub fn mark_stale(&self) {
        self.inner.generation.fetch_add(1, Ordering::SeqCst);
        PoolInner::spawn_refresher(&self.inner);
    }

    /// `None` means: run it in-process (no worker, or a non-crash error).
    /// `Some(Err)` after a crash must not fall back: it would crash the app.
    pub fn run(&self, sql: &str) -> Option<Result<Vec<u8>, AppError>> {
        let inner = &self.inner;
        let mut guard = inner.acquire()?;
        let threads = inner.threads;

        // Left in single-thread mode by a crash retry.
        if guard.as_ref().is_some_and(|w| w.threads != threads) {
            let generation = guard.as_ref()?.generation;
            if let Err(e) = guard.as_mut()?.respawn(threads, generation) {
                eprintln!("[query-worker] restart failed: {e}");
                *guard = None;
                return None;
            }
        }

        let worker = guard.as_mut()?;
        let generation = worker.generation;
        match worker.execute(sql) {
            Ok(data) => Some(Ok(data)),
            Err(e) if is_crash(&e) => {
                // The known DuckDB crash is multi-threaded string compression.
                eprintln!("[query-worker] crashed, retrying with 1 thread");
                if let Err(re) = worker.respawn(1, generation) {
                    eprintln!("[query-worker] restart failed: {re}");
                    *guard = None;
                    return Some(Err(e));
                }
                match worker.execute(sql) {
                    Ok(data) => Some(Ok(data)),
                    Err(e2) if is_crash(&e2) => {
                        if let Err(re) = worker.respawn(threads, generation) {
                            eprintln!("[query-worker] restart failed: {re}");
                            *guard = None;
                        }
                        Some(Err(AppError::QueryError(CRASH_MESSAGE.to_string())))
                    }
                    Err(e2) => {
                        eprintln!("[query-worker] {e2}; running in-process");
                        None
                    }
                }
            }
            Err(e) => {
                eprintln!("[query-worker] {e}; running in-process");
                None
            }
        }
    }
}

impl PoolInner {
    /// A free worker, else wait for a busy one. None when no worker runs.
    fn acquire(&self) -> Option<MutexGuard<'_, Option<QueryWorker>>> {
        let n = self.slots.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        for i in 0..n {
            if let Some(g) = self.slots[(start + i) % n].try_lock() {
                if g.is_some() {
                    return Some(g);
                }
            }
        }
        for i in 0..n {
            let g = self.slots[(start + i) % n].lock();
            if g.is_some() {
                return Some(g);
            }
        }
        None
    }

    /// Spawn new workers without holding the slot locks, then swap them in.
    fn replace_all(&self, generation: u64) {
        let Some(catalog_path) = self.catalog_path.lock().clone() else {
            return;
        };
        std::thread::scope(|scope| {
            for slot in &self.slots {
                if slot.lock().as_ref().is_some_and(|w| w.generation >= generation) {
                    continue;
                }
                let catalog_path = catalog_path.clone();
                scope.spawn(move || match QueryWorker::spawn(catalog_path, self.threads, generation) {
                    Ok(fresh) => {
                        let mut g = slot.lock();
                        if !g.as_ref().is_some_and(|w| w.generation >= generation) {
                            *g = Some(fresh);
                        }
                    }
                    Err(e) => eprintln!("[query-worker] failed to start: {e}"),
                });
            }
        });
    }

    fn spawn_refresher(this: &Arc<Self>) {
        if this.refreshing.swap(true, Ordering::SeqCst) {
            return;
        }
        let inner = Arc::clone(this);
        std::thread::spawn(move || loop {
            std::thread::sleep(REFRESH_DEBOUNCE);
            let generation = inner.generation.load(Ordering::SeqCst);
            inner.replace_all(generation);
            inner.refreshing.store(false, Ordering::SeqCst);
            // Run again if a change landed during the refresh.
            if inner.generation.load(Ordering::SeqCst) == generation
                || inner.refreshing.swap(true, Ordering::SeqCst)
            {
                break;
            }
        });
    }
}

/// Call after saving a catalog change that adds, removes, or renames views.
pub fn reload(state: &crate::state::AppState) {
    state.query_workers.mark_stale();
}

impl Drop for QueryWorker {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

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

    let (catalog_path, threads) = match req {
        Request::Init { catalog_path, threads } => (catalog_path, threads),
        _ => {
            let _ = write_err_response(&mut stdout, "expected init message");
            std::process::exit(1);
        }
    };

    let conn = match init_connection(&catalog_path, threads) {
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
                let _ = match exec_query(&conn, &sql) {
                    Ok(ipc) => write_ok(&mut stdout, &ipc),
                    Err(msg) => write_err_response(&mut stdout, &msg),
                };
            }
            _ => {
                let _ = write_err_response(&mut stdout, "unexpected message type");
            }
        }
    }
}

/// The worker cannot open the app's DuckDB file (the app holds an exclusive
/// lock), so materialized tables are not visible here and run in-process.
fn init_connection(catalog_path: &str, threads: u32) -> Result<duckdb::Connection, String> {
    let conn = duckdb::Connection::open_in_memory().map_err(|e| format!("open_in_memory: {e}"))?;
    conn.execute_batch(&format!("SET threads TO {}", threads.max(1))).ok();
    conn.execute_batch("INSTALL excel; LOAD excel;").ok();

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::Instant;

    // Uses real worker processes:
    // CDV_WORKER_EXE=target/debug/cdv cargo test pool_ -- --ignored

    fn write_catalog(catalog: &Path, data_dir: &Path, files: &[&str]) {
        let files: Vec<String> = files
            .iter()
            .map(|f| data_dir.join(f).to_string_lossy().into_owned())
            .collect();
        let cat = serde_json::json!({
            "connectors": [{
                "id": "d1", "name": "lake", "connector_type": "directory", "alias": "lake",
                "config": { "path": data_dir.to_string_lossy(), "files": files }
            }]
        });
        std::fs::write(catalog, serde_json::to_vec(&cat).unwrap()).unwrap();
    }

    fn wait_for_refresh(pool: &WorkerPool) {
        let t = Instant::now();
        while pool.inner.refreshing.load(Ordering::SeqCst) && t.elapsed() < Duration::from_secs(20) {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn pids(pool: &WorkerPool) -> Vec<u32> {
        pool.inner.slots.iter().map(|s| s.lock().as_ref().unwrap().child.id()).collect()
    }

    #[test]
    #[ignore]
    fn pool_runs_queries_in_parallel_and_reloads() {
        let dir = std::env::temp_dir().join(format!("cdv_pool_{}", uuid::Uuid::new_v4()));
        let data = dir.join("data");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("a.csv"), "id\n1\n2\n").unwrap();
        std::fs::write(data.join("b.csv"), "id\n9\n").unwrap();
        let catalog = dir.join("catalog.json");
        write_catalog(&catalog, &data, &["a.csv"]);

        let pool = WorkerPool::new(2);
        pool.start(catalog.clone());
        assert!(pool.inner.slots.iter().all(|s| s.lock().is_some()));

        assert!(matches!(pool.run("SELECT * FROM lake.main.a"), Some(Ok(_))));
        assert!(pool.run("SELECT * FROM lake.main.b").is_none());

        write_catalog(&catalog, &data, &["a.csv", "b.csv"]);
        pool.mark_stale();
        // Old workers keep serving during the refresh.
        assert!(matches!(pool.run("SELECT * FROM lake.main.a"), Some(Ok(_))));
        wait_for_refresh(&pool);
        for _ in 0..2 {
            assert!(matches!(pool.run("SELECT * FROM lake.main.b"), Some(Ok(_))));
        }

        // A burst of changes costs one refresh round.
        let before = pids(&pool);
        for _ in 0..10 {
            pool.mark_stale();
        }
        std::thread::sleep(Duration::from_millis(100));
        wait_for_refresh(&pool);
        assert!(before.iter().zip(pids(&pool)).all(|(a, b)| *a != b));
        let generation = pool.inner.generation.load(Ordering::SeqCst);
        assert!(pool.inner.slots.iter().all(|s| s.lock().as_ref().unwrap().generation == generation));

        // A long query on one worker does not block a short one.
        let slow = "SELECT COUNT(*) FROM range(400000000) t(x) WHERE x % 7 = 3";
        std::thread::scope(|scope| {
            let t0 = Instant::now();
            let h = scope.spawn(|| pool.run(slow));
            std::thread::sleep(Duration::from_millis(200));
            let t1 = Instant::now();
            assert!(matches!(pool.run("SELECT 1"), Some(Ok(_))));
            let short = t1.elapsed();
            assert!(matches!(h.join().unwrap(), Some(Ok(_))));
            assert!(short < t0.elapsed() / 2, "short query waited for the long one");
        });

        std::fs::remove_dir_all(&dir).ok();
    }
}
