# Changelog

All notable changes to CDV are documented in this file.

## [0.0.2] — 2026-04-01

### Added

- **Database connectors** — attach PostgreSQL, Snowflake, SQLite, and other DuckDB databases as queryable sources alongside local files
- **DuckLake support** — attach DuckLake catalogs and browse their tables/columns in the sidebar
- **Snowflake via ADBC** — ADBC driver is auto-downloaded on first connection; multi-database attach
- **ETL jobs** — sync data from PostgreSQL sources into DuckLake targets with progress tracking and cancellation
- **Graph analytics (DuckPGQ)** — install the DuckPGQ extension from the app, define property graphs over data sources, run PageRank / local clustering coefficient / weakly connected components, and visualize results with a force-directed graph
- **Avro format** — `.avro` files can now be imported as data sources
- **Data source refresh** — re-read a source from disk or remote without removing and re-adding it
- **Connector table/column introspection** — browse schemas, tables, and columns of attached databases in the sidebar

### Changed

- Restructured data source and connector internals for the new connector types

## [0.0.1] — 2026-03-08

Initial release.

### Supported Formats

**Import:** CSV, TSV, JSON, JSONL, Parquet, Excel (`.xlsx`), Arrow IPC (`.arrow`, `.ipc`)
**Export:** CSV, Parquet, JSON
**Cloud:** S3 (+ compatible), Google Cloud Storage, Cloudflare R2

### Features

- Embedded DuckDB engine — SQL over local and remote files, no external database
- Arrow IPC transport between Rust backend and React frontend
- Monaco SQL editor with context-aware autocomplete
- Virtualized data table with column sorting, resizing, type-aware rendering
- Server-side pagination with configurable page size and LRU page cache
- Streaming query execution for large result sets
- Data source management — local files, cloud, drag-and-drop, schema preview, materialize or view
- Cloud connections with credential management and remote file browsing
- Multi-tab workspace with persistence across restarts
- Export query results to CSV, Parquet, or JSON
- Settings panel with auto-persist
