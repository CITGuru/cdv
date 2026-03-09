# Changelog

All notable changes to CDV are documented in this file.

## [0.0.1] — 2026-03-08

Initial release of **Columnar Data Viewer** — a desktop data exploration and SQL query tool for flat and columnar file formats, powered by DuckDB.

### Supported Data Formats

**Import:** CSV, TSV, JSON, JSONL, Parquet, Excel (`.xlsx`), Arrow IPC (`.arrow`, `.ipc`)
**Export:** CSV, Parquet, JSON
**Cloud Storage:** Amazon S3 (+ S3-compatible), Google Cloud Storage, Cloudflare R2

### Features

- **Embedded DuckDB engine** — full SQL support over local and remote files with no external database required
- **Arrow IPC data transport** — high-performance columnar data transfer between the Rust backend and React frontend
- **Monaco SQL editor** — syntax highlighting, context-aware autocomplete for table names, column names, and dot-notation, Ctrl+Enter execution
- **Virtualized data table** — smooth scrolling over large datasets with column sorting, resizing, and type-aware cell rendering (NULL, boolean, number formatting)
- **Server-side pagination** — configurable page size (100 / 500 / 1000) with LRU page caching (up to 30 pages)
- **Streaming query execution** — chunked result delivery for large result sets via Tauri events
- **Data source management** — add from local files or cloud, drag-and-drop import, auto-format detection, schema preview, materialize as table or keep as lazy view, update/reimport/remove sources
- **Cloud connections** — S3/GCS/R2 with credentials, browse remote files via glob, import directly as data sources
- **Multi-tab workspace** — data tabs and query tabs with full state persistence across restarts, double-click rename for query tabs
- **Sidebar** — tree view of data sources with expandable columns and type annotations, format-specific icons, right-click context menu (New Query, View Data, Drop, Export, Import, Properties), connections section, query history
- **Schema panel** — collapsible bar showing dataset name, view name, format, column count, row count, and full column schema
- **Data export** — export query results to CSV, Parquet, or JSON via native save dialog
- **Settings** — sidebar width, default page size, max rows, streaming toggle/threshold, default export format, all auto-persisted
- **Structured error handling** — backend error codes, frontend classification with contextual fix suggestions, color-coded display with copy-to-clipboard
