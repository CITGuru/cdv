# Contributing to CDV

Thanks for your interest in contributing. Here's how to get started.

## Getting set up

1. Fork and clone the repo.
2. Install prerequisites: [Rust](https://www.rust-lang.org/tools/install), [Bun](https://bun.sh/) (or Node.js), and the [Tauri CLI](https://v2.tauri.app/start/prerequisites/).
3. Install dependencies and start the dev server:

```bash
bun install
bun tauri dev
```

The app should open in a native window. The frontend hot-reloads; Rust changes require a restart.

## Project structure

```
src/                  # React + TypeScript frontend
  components/         # UI components (sidebar, query editor, data table, modals, etc.)
  hooks/              # React hooks (connectors, datasets, queries, ETL, settings)
  lib/                # IPC bindings, Arrow helpers, types
src-tauri/
  src/                # Rust backend
    lib.rs            # Tauri setup and command registration
    state.rs          # App state, connector/data source types
    connector.rs      # Database connector logic (Postgres, Snowflake, SQLite, DuckDB, DuckLake)
    dataset_manager.rs # File import, schema, views/tables
    query_engine.rs   # SQL execution, pagination, streaming
    graph.rs          # DuckPGQ property graphs and algorithms
    etl.rs            # PostgreSQL → DuckLake sync jobs
    export_service.rs # COPY-based export
    catalog.rs        # catalog.json persistence
    settings.rs       # settings.json persistence
    workspace.rs      # workspace.json (tabs)
    cloud_connector.rs # S3/GCS/R2 cloud connections
    error.rs          # Error types
```

## Making changes

1. Create a branch off `main`.
2. Keep commits focused — one logical change per commit.
3. Make sure the app builds cleanly before opening a PR:

```bash
bun tauri build
```

4. If you're adding a new Tauri command, register it in `lib.rs` and add the corresponding IPC binding in `src/lib/ipc.ts`.
5. If you're adding a new data format or connector type, update `state.rs` (Rust types) and `src/lib/types.ts` (TypeScript types) to keep them in sync.

## Pull requests

- Open a PR against `main`.
- Describe what the change does and why.
- Keep the diff as small as reasonable — large PRs are harder to review.
- If the change is user-facing, mention it so the changelog can be updated.

## Reporting bugs

Open an issue with:
- What you expected to happen.
- What actually happened.
- Steps to reproduce if possible.
- OS and CDV version.

## Code style

- **Rust**: follow standard `rustfmt` conventions.
- **TypeScript/React**: the project uses Tailwind CSS and shadcn/ui components. Keep new UI consistent with existing patterns.
- Avoid unnecessary comments — the code should speak for itself.

## License

By contributing you agree that your contributions will be licensed under the [MIT License](LICENSE).
