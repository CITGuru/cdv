//! Directory connector: every supported data file under a root directory becomes a
//! DuckDB view.
//!
//! Layout inside DuckDB: the connector attaches an in-memory database under
//! its alias. Each subdirectory becomes a schema and each file becomes a view in
//! that schema:
//!
//!   <root>/demo_bank/CUSTOMER.parquet  ->  "<alias>"."demo_bank"."customer"
//!   <root>/truth/events.parquet        ->  "<alias>"."truth"."events"
//!   <root>/dataset_card.json           ->  "<alias>"."main"."dataset_card"
//!
//! A JSON manifest (path in `config.metadata_path`, inside the app data
//! directory) records every file found by the last scan, with its size, modified
//! time, schema and row count. Activation (app start, query worker start)
//! builds views from the manifest without a full rescan. A refresh rescans
//! the directory and describes again only new or changed files.
//!
//! Hive partition directories (`key=value`) are skipped for now. The manifest
//! `kind` field leaves room to add them later as one table per partition root.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use duckdb::Connection;
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::state::{CatalogEntry, ColumnInfo, Connector};

const MANIFEST_VERSION: u32 = 1;
const MAX_DEPTH: usize = 16;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct DirectoryManifest {
    pub version: u32,
    pub root: String,
    /// Unix time in milliseconds of the last scan.
    pub scanned_at: i64,
    pub files: Vec<ManifestFile>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManifestFile {
    /// Path relative to the root, with `/` separators.
    pub path: String,
    /// "file" for now; "partitioned" is reserved for hive partition roots.
    #[serde(default = "default_file_kind")]
    pub kind: String,
    pub format: String,
    pub size: u64,
    pub modified_ms: i64,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub columns: Vec<ColumnInfo>,
    #[serde(default)]
    pub row_count: Option<u64>,
    /// Set when the file could not be read; the file then has no view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn default_file_kind() -> String {
    "file".to_string()
}

struct ScannedFile {
    rel_path: String,
    format: String,
    size: u64,
    modified_ms: i64,
}

// ─── Scan ──────────────────────────────────────────────────────────

fn ext_to_format(ext: &str) -> Option<&'static str> {
    match ext {
        "csv" => Some("csv"),
        "tsv" => Some("tsv"),
        "json" => Some("json"),
        "jsonl" | "ndjson" => Some("jsonl"),
        "parquet" => Some("parquet"),
        "xlsx" => Some("xlsx"),
        "avro" => Some("avro"),
        "arrow" | "ipc" | "feather" => Some("arrow_ipc"),
        _ => None,
    }
}

fn is_ignored_name(name: &str) -> bool {
    // Hidden files and Spark/Hadoop markers (_SUCCESS, _committed_*, ...).
    name.starts_with('.') || name.starts_with('_')
}

fn is_partition_dir(name: &str) -> bool {
    matches!(name.split_once('='), Some((k, v)) if !k.is_empty() && !v.is_empty())
}

fn scan_directory(root: &Path) -> Result<Vec<ScannedFile>, AppError> {
    if !root.is_dir() {
        return Err(AppError::FileError(format!(
            "Directory not found: {}",
            root.display()
        )));
    }
    let mut out = Vec::new();
    scan_dir(root, root, 0, &mut out);
    out.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(out)
}

fn scan_dir(root: &Path, dir: &Path, depth: usize, out: &mut Vec<ScannedFile>) {
    if depth > MAX_DEPTH {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        eprintln!("[directory] cannot read {}", dir.display());
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if is_ignored_name(&name) {
            continue;
        }
        // `file_type` does not follow symlinks, so symlink loops are not walked.
        let Ok(ft) = entry.file_type() else { continue };
        let path = entry.path();
        if ft.is_dir() {
            if is_partition_dir(&name) {
                // TODO: hive partition support (one table per partition root).
                continue;
            }
            scan_dir(root, &path, depth + 1, out);
        } else if ft.is_file() {
            if let Some(f) = scanned_file(root, &path) {
                out.push(f);
            }
        }
    }
}

/// Metadata for one supported file, or None when the format is not
/// supported or the file cannot be read.
fn scanned_file(root: &Path, path: &Path) -> Option<ScannedFile> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    let format = ext_to_format(&ext)?;
    let meta = std::fs::metadata(path).ok()?;
    if !meta.is_file() {
        return None;
    }
    let modified_ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let rel_path = path
        .strip_prefix(root)
        .unwrap_or(path)
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/");
    Some(ScannedFile {
        rel_path,
        format: format.to_string(),
        size: meta.len(),
        modified_ms,
    })
}

/// Only the listed files (multiple-file selection). A refresh never adds
/// other files from the root. Files that no longer exist are left out.
fn scan_listed(root: &Path, files: &[String]) -> Vec<ScannedFile> {
    let mut out: Vec<ScannedFile> = files
        .iter()
        .filter_map(|f| {
            let p = Path::new(f);
            let s = scanned_file(root, p);
            if s.is_none() {
                eprintln!("[directory] listed file missing or unsupported: {}", f);
            }
            s
        })
        .collect();
    out.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    out.dedup_by(|a, b| a.rel_path == b.rel_path);
    out
}

fn scan_source(connector: &Connector, root: &Path) -> Result<Vec<ScannedFile>, AppError> {
    match connector.config.files.as_deref() {
        Some(files) => Ok(scan_listed(root, files)),
        None => scan_directory(root),
    }
}

/// Longest shared parent folder of the given file paths.
fn common_parent(files: &[String]) -> Option<PathBuf> {
    let mut iter = files.iter().map(|f| Path::new(f).parent().map(Path::to_path_buf));
    let mut common = iter.next()??;
    for p in iter {
        let p = p?;
        while !p.starts_with(&common) {
            common = common.parent()?.to_path_buf();
        }
    }
    Some(common)
}

/// Check and complete the config of a new directory connector. For a
/// multiple-file selection, the root becomes the shared parent folder.
pub fn prepare_config(config: &mut crate::state::ConnectorConfig) -> Result<(), AppError> {
    if let Some(files) = config.files.as_mut() {
        files.retain(|f| !f.trim().is_empty());
        if files.is_empty() {
            return Err(AppError::ConnectorError("No files selected".into()));
        }
        let root = common_parent(files).ok_or_else(|| {
            AppError::ConnectorError("Selected files have no shared parent folder".into())
        })?;
        config.path = Some(root.to_string_lossy().into_owned());
    } else {
        let root = config
            .path
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| AppError::ConnectorError("Directory path is empty".into()))?;
        let root = root.trim_end_matches(['/', '\\']);
        let root = if root.is_empty() { "/" } else { root };
        if !Path::new(root).is_dir() {
            return Err(AppError::ConnectorError(format!("Directory not found: {}", root)));
        }
        config.path = Some(root.to_string());
    }
    Ok(())
}

// ─── Naming ────────────────────────────────────────────────────────

fn sanitize_ident(s: &str) -> String {
    let mapped: String = s
        .chars()
        .map(|c| if c.is_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
        .collect();
    let mut collapsed = String::with_capacity(mapped.len());
    for c in mapped.chars() {
        if c == '_' && collapsed.ends_with('_') {
            continue;
        }
        collapsed.push(c);
    }
    let trimmed = collapsed.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "untitled".to_string()
    } else if trimmed.chars().next().is_some_and(|c| c.is_numeric()) {
        format!("t_{}", trimmed)
    } else {
        trimmed
    }
}

/// Schema and table name for each file, in input order. Root files go to
/// `main`. Nested directories join with `_` (`a/b/x.csv` -> schema `a_b`).
/// Name clashes in one schema get the format, then a number, as a suffix.
fn assign_names(files: &[ScannedFile]) -> Vec<(String, String)> {
    let mut used: HashSet<(String, String)> = HashSet::new();
    files
        .iter()
        .map(|f| {
            let mut parts: Vec<&str> = f.rel_path.split('/').collect();
            let file_name = parts.pop().unwrap_or("untitled");
            let schema = if parts.is_empty() {
                "main".to_string()
            } else {
                sanitize_ident(&parts.join("_"))
            };
            let stem = Path::new(file_name)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(file_name);
            let base = sanitize_ident(stem);
            let mut table = base.clone();
            if used.contains(&(schema.clone(), table.clone())) {
                table = format!("{}_{}", base, sanitize_ident(&f.format));
            }
            let mut n = 2;
            while used.contains(&(schema.clone(), table.clone())) {
                table = format!("{}_{}", base, n);
                n += 1;
            }
            used.insert((schema.clone(), table.clone()));
            (schema, table)
        })
        .collect()
}

// ─── Manifest IO ───────────────────────────────────────────────────

pub fn manifest_path_for(app_data_dir: &Path, connector_id: &str) -> PathBuf {
    app_data_dir
        .join("directory_manifests")
        .join(format!("{}.json", connector_id))
}

fn load_manifest(path: &Path) -> Option<DirectoryManifest> {
    let data = std::fs::read_to_string(path).ok()?;
    let m: DirectoryManifest = serde_json::from_str(&data).ok()?;
    (m.version == MANIFEST_VERSION).then_some(m)
}

fn save_manifest(path: &Path, m: &DirectoryManifest) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| AppError::FileError(format!("create manifest dir: {}", e)))?;
    }
    let data = serde_json::to_vec_pretty(m)
        .map_err(|e| AppError::FileError(format!("encode manifest: {}", e)))?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, data)
        .map_err(|e| AppError::FileError(format!("write manifest: {}", e)))?;
    std::fs::rename(&tmp, path)
        .map_err(|e| AppError::FileError(format!("write manifest: {}", e)))?;
    Ok(())
}

// ─── DuckDB objects ────────────────────────────────────────────────

fn q(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn connector_parts(connector: &Connector) -> Result<(&str, &str), AppError> {
    let root = connector
        .config
        .path
        .as_deref()
        .ok_or_else(|| AppError::ConnectorError("Directory connector missing path".into()))?;
    let alias = connector
        .alias
        .as_deref()
        .ok_or_else(|| AppError::ConnectorError("Directory connector missing alias".into()))?;
    Ok((root, alias))
}

/// Drop and attach a fresh in-memory database under `alias`, so that views
/// for deleted files do not stay behind.
fn reset_attach(conn: &Connection, alias: &str) -> Result<(), AppError> {
    conn.execute_batch(&format!("DETACH DATABASE IF EXISTS {}", q(alias)))
        .ok();
    conn.execute_batch(&format!("ATTACH ':memory:' AS {}", q(alias)))
        .map_err(|e| AppError::ConnectorError(format!("Failed to attach directory: {}", e)))?;
    Ok(())
}

fn create_view(
    conn: &Connection,
    alias: &str,
    root: &Path,
    f: &ManifestFile,
) -> Result<String, AppError> {
    let abs = root.join(&f.path);
    let (_, duckdb_ref) =
        crate::dataset_manager::format_to_ref(&abs.to_string_lossy(), &f.format)?;
    let schema_q = format!("{}.{}", q(alias), q(&f.schema));
    let view_q = format!("{}.{}", schema_q, q(&f.table));
    conn.execute_batch(&format!(
        "CREATE SCHEMA IF NOT EXISTS {schema_q}; \
         CREATE OR REPLACE VIEW {view_q} AS SELECT * FROM {duckdb_ref};"
    ))?;
    Ok(view_q)
}

fn manifest_from_scan(root: &str, scanned: &[ScannedFile]) -> DirectoryManifest {
    let names = assign_names(scanned);
    DirectoryManifest {
        version: MANIFEST_VERSION,
        root: root.to_string(),
        scanned_at: chrono::Utc::now().timestamp_millis(),
        files: scanned
            .iter()
            .zip(names)
            .map(|(s, (schema, table))| ManifestFile {
                path: s.rel_path.clone(),
                kind: default_file_kind(),
                format: s.format.clone(),
                size: s.size,
                modified_ms: s.modified_ms,
                schema,
                table,
                columns: Vec::new(),
                row_count: None,
                error: None,
            })
            .collect(),
    }
}

/// Build the views for a directory connector. Uses the manifest when it is
/// present and matches the root; otherwise scans the directory (names only, no
/// describe). Never writes the manifest, so the query worker can call it.
pub fn activate(conn: &Connection, connector: &Connector) -> Result<(), AppError> {
    let (root, alias) = connector_parts(connector)?;
    let manifest = connector
        .config
        .metadata_path
        .as_deref()
        .and_then(|p| load_manifest(Path::new(p)))
        .filter(|m| m.root == root);
    let manifest = match manifest {
        Some(m) => m,
        None => manifest_from_scan(root, &scan_source(connector, Path::new(root))?),
    };

    reset_attach(conn, alias)?;
    let root_path = Path::new(root);
    for f in manifest.files.iter().filter(|f| f.error.is_none()) {
        if let Err(e) = create_view(conn, alias, root_path, f) {
            eprintln!("[directory] view for {} failed: {}", f.path, e);
        }
    }
    Ok(())
}

pub fn deactivate(conn: &Connection, connector: &Connector) -> Result<(), AppError> {
    if let Some(alias) = connector.alias.as_deref() {
        conn.execute_batch(&format!("DETACH DATABASE IF EXISTS {}", q(alias)))?;
    }
    Ok(())
}

/// Rescan the directory, rebuild all views, describe new or changed files,
/// write the manifest, and return the catalog for the sidebar tree.
pub fn refresh(conn: &Connection, connector: &Connector) -> Result<Vec<CatalogEntry>, AppError> {
    let (root, alias) = connector_parts(connector)?;
    let root_path = Path::new(root);
    let scanned = scan_source(connector, root_path)?;
    let mut manifest = manifest_from_scan(root, &scanned);

    let manifest_path = connector.config.metadata_path.as_deref().map(Path::new);
    let previous: HashMap<String, ManifestFile> = manifest_path
        .and_then(load_manifest)
        .filter(|m| m.root == root)
        .map(|m| m.files.into_iter().map(|f| (f.path.clone(), f)).collect())
        .unwrap_or_default();

    reset_attach(conn, alias)?;

    let mut reused = 0usize;
    let mut described = 0usize;
    for f in manifest.files.iter_mut() {
        let view_q = match create_view(conn, alias, root_path, f) {
            Ok(v) => v,
            Err(e) => {
                f.error = Some(e.to_string());
                eprintln!("[directory] skip {}: {}", f.path, f.error.as_deref().unwrap_or(""));
                continue;
            }
        };
        let unchanged = previous.get(&f.path).filter(|p| {
            p.error.is_none()
                && !p.columns.is_empty()
                && p.size == f.size
                && p.modified_ms == f.modified_ms
                && p.format == f.format
        });
        if let Some(p) = unchanged {
            f.columns = p.columns.clone();
            f.row_count = p.row_count;
            reused += 1;
            continue;
        }
        match crate::dataset_manager::describe_ref(conn, &view_q) {
            Ok(cols) => {
                f.columns = cols;
                f.row_count = crate::dataset_manager::count_ref(conn, &view_q);
                described += 1;
            }
            Err(e) => {
                f.error = Some(e.to_string());
                conn.execute_batch(&format!("DROP VIEW IF EXISTS {}", view_q)).ok();
                eprintln!("[directory] skip {}: {}", f.path, e);
            }
        }
    }
    eprintln!(
        "[directory] scanned {}: {} files ({} unchanged, {} described, {} failed)",
        root,
        manifest.files.len(),
        reused,
        described,
        manifest.files.iter().filter(|f| f.error.is_some()).count()
    );

    if let Some(p) = manifest_path {
        if let Err(e) = save_manifest(p, &manifest) {
            eprintln!("[directory] {}", e);
        }
    }

    Ok(manifest
        .files
        .into_iter()
        .filter(|f| f.error.is_none())
        .map(|f| CatalogEntry {
            schema: Some(f.schema),
            name: f.table,
            entry_type: "view".to_string(),
            columns: f.columns,
            row_count: f.row_count,
        })
        .collect())
}

pub fn test(connector: &Connector) -> Result<(), AppError> {
    let root = connector
        .config
        .path
        .as_deref()
        .ok_or_else(|| AppError::ConnectorError("Directory connector missing path".into()))?;
    if !Path::new(root).is_dir() {
        return Err(AppError::ConnectorError(format!("Directory not found: {}", root)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sf(rel: &str, format: &str) -> ScannedFile {
        ScannedFile {
            rel_path: rel.to_string(),
            format: format.to_string(),
            size: 0,
            modified_ms: 0,
        }
    }

    #[test]
    fn names_follow_directory_tree() {
        let files = vec![
            sf("dataset_card.json", "json"),
            sf("demo_bank/CUSTOMER.parquet", "parquet"),
            sf("demo_bank/LOAN-APP.parquet", "parquet"),
            sf("truth/sub dir/events.parquet", "parquet"),
        ];
        let names = assign_names(&files);
        assert_eq!(names[0], ("main".into(), "dataset_card".into()));
        assert_eq!(names[1], ("demo_bank".into(), "customer".into()));
        assert_eq!(names[2], ("demo_bank".into(), "loan_app".into()));
        assert_eq!(names[3], ("truth_sub_dir".into(), "events".into()));
    }

    #[test]
    fn clashing_names_get_suffix() {
        let files = vec![
            sf("a/x.csv", "csv"),
            sf("a/x.parquet", "parquet"),
            sf("a/X.parquet", "parquet"),
        ];
        let names = assign_names(&files);
        assert_eq!(names[0].1, "x");
        assert_eq!(names[1].1, "x_parquet");
        assert_eq!(names[2].1, "x_2");
    }

    #[test]
    fn partition_dirs_detected() {
        assert!(is_partition_dir("year=2024"));
        assert!(!is_partition_dir("year="));
        assert!(!is_partition_dir("demo_bank"));
    }

    fn directory_connector(root: &Path, manifest: &Path) -> Connector {
        Connector {
            id: "t".into(),
            name: "t".into(),
            connector_type: crate::state::ConnectorType::Directory,
            config: crate::state::ConnectorConfig {
                path: Some(root.to_string_lossy().into_owned()),
                metadata_path: Some(manifest.to_string_lossy().into_owned()),
                ..Default::default()
            },
            alias: Some("lake".into()),
            secret_name: None,
        }
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn scan_query_and_manifest_roundtrip() {
        let dir = std::env::temp_dir().join(format!("cdv_directory_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("bank/2024")).unwrap();
        std::fs::create_dir_all(dir.join("year=2024")).unwrap();
        std::fs::write(dir.join("card.csv"), "a,b\n1,2\n").unwrap();
        std::fs::write(dir.join("bank/customer.csv"), "id,name\n1,x\n2,y\n").unwrap();
        std::fs::write(dir.join("bank/2024/loans.csv"), "id\n1\n2\n3\n").unwrap();
        std::fs::write(dir.join("bank/notes.txt"), "skip").unwrap();
        std::fs::write(dir.join(".hidden.csv"), "a\n1\n").unwrap();
        std::fs::write(dir.join("year=2024/p.csv"), "a\n1\n").unwrap();
        let manifest = dir.join("manifest.json");
        let c = directory_connector(&dir, &manifest);

        let conn = Connection::open_in_memory().unwrap();
        let cat = refresh(&conn, &c).unwrap();
        let mut names: Vec<String> = cat
            .iter()
            .map(|e| format!("{}.{}", e.schema.as_deref().unwrap(), e.name))
            .collect();
        names.sort();
        assert_eq!(names, vec!["bank.customer", "bank_2024.loans", "main.card"]);
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM lake.bank_2024.loans"), 3);
        assert_eq!(
            count(&conn, "SELECT COUNT(*) FROM lake.bank.customer c JOIN lake.bank_2024.loans l USING (id)"),
            2
        );

        let m = load_manifest(&manifest).unwrap();
        assert_eq!(m.files.len(), 3);
        assert_eq!(m.files.iter().find(|f| f.table == "loans").unwrap().row_count, Some(3));

        // A second connection (like the query worker) builds views from the
        // manifest alone.
        let conn2 = Connection::open_in_memory().unwrap();
        activate(&conn2, &c).unwrap();
        assert_eq!(count(&conn2, "SELECT COUNT(*) FROM lake.bank.customer"), 2);

        // Refresh picks up removed and added files.
        std::fs::remove_file(dir.join("card.csv")).unwrap();
        std::fs::write(dir.join("bank/new.csv"), "z\n9\n").unwrap();
        let cat = refresh(&conn, &c).unwrap();
        let names: HashSet<String> = cat.iter().map(|e| e.name.clone()).collect();
        assert!(names.contains("new") && !names.contains("card"));
        assert!(conn.prepare("SELECT * FROM lake.main.card").is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn listed_files_only() {
        let dir = std::env::temp_dir().join(format!("cdv_listed_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("bank/sub")).unwrap();
        std::fs::create_dir_all(dir.join("truth")).unwrap();
        let a = dir.join("bank/customer.csv");
        let b = dir.join("bank/sub/loans.csv");
        std::fs::write(&a, "id\n1\n").unwrap();
        std::fs::write(&b, "id\n1\n2\n").unwrap();
        std::fs::write(dir.join("bank/other.csv"), "id\n1\n").unwrap();
        std::fs::write(dir.join("truth/events.csv"), "id\n1\n").unwrap();

        let mut cfg = crate::state::ConnectorConfig {
            files: Some(vec![
                a.to_string_lossy().into_owned(),
                b.to_string_lossy().into_owned(),
            ]),
            ..Default::default()
        };
        prepare_config(&mut cfg).unwrap();
        assert_eq!(cfg.path.as_deref(), Some(dir.join("bank").to_string_lossy().as_ref()));

        let manifest = dir.join("manifest.json");
        let mut c = directory_connector(&dir, &manifest);
        c.config.path = cfg.path.clone();
        c.config.files = cfg.files.clone();

        let conn = Connection::open_in_memory().unwrap();
        let names = |cat: Vec<CatalogEntry>| {
            let mut v: Vec<String> = cat
                .into_iter()
                .map(|e| format!("{}.{}", e.schema.unwrap(), e.name))
                .collect();
            v.sort();
            v
        };
        assert_eq!(names(refresh(&conn, &c).unwrap()), vec!["main.customer", "sub.loans"]);

        // New files next to the listed ones do not appear on refresh, and a
        // deleted listed file drops out.
        std::fs::write(dir.join("bank/new.csv"), "id\n1\n").unwrap();
        std::fs::remove_file(&a).unwrap();
        assert_eq!(names(refresh(&conn, &c).unwrap()), vec!["sub.loans"]);
        assert_eq!(load_manifest(&manifest).unwrap().files.len(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn directory_path_checks() {
        let mut cfg = crate::state::ConnectorConfig {
            path: Some("/definitely/not/here".into()),
            ..Default::default()
        };
        assert!(prepare_config(&mut cfg).is_err());
        let tmp = std::env::temp_dir();
        let mut cfg = crate::state::ConnectorConfig {
            path: Some(format!("  {}/  ", tmp.to_string_lossy().trim_end_matches('/'))),
            ..Default::default()
        };
        prepare_config(&mut cfg).unwrap();
        assert_eq!(cfg.path.as_deref(), Some(tmp.to_string_lossy().trim_end_matches('/')));
    }

    /// Manual check against a real directory: CDV_DIRECTORY=/path cargo test -- --ignored real_directory --nocapture
    #[test]
    #[ignore]
    fn real_directory() {
        let root = PathBuf::from(std::env::var("CDV_DIRECTORY").expect("set CDV_DIRECTORY"));
        let manifest = std::env::temp_dir().join(format!("cdv_manifest_{}.json", uuid::Uuid::new_v4()));
        let c = directory_connector(&root, &manifest);
        let conn = Connection::open_in_memory().unwrap();
        for e in refresh(&conn, &c).unwrap() {
            println!("{}.{}  cols={} rows={:?}", e.schema.unwrap(), e.name, e.columns.len(), e.row_count);
        }
        println!("{}", std::fs::read_to_string(&manifest).unwrap().lines().take(25).collect::<Vec<_>>().join("\n"));
        std::fs::remove_file(&manifest).ok();
    }
}
