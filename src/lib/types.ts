export interface ColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
  key?: string | null;
}

export type ConnectorType =
  | "local_file"
  | "sqlite"
  | "duckdb"
  | "postgresql"
  | "snowflake"
  | "s3"
  | "gcs"
  | "r2"
  | "ducklake";

export type Driver = "duckdb" | "chdb";

export const DRIVER_OPTIONS: { value: Driver; label: string }[] = [
  { value: "duckdb", label: "DuckDB" },
  { value: "chdb", label: "chDB" },
];

export function supportedDrivers(connectorType: ConnectorType): Driver[] {
  switch (connectorType) {
    case "local_file":
    case "duckdb":
    case "sqlite":
    case "s3":
    case "gcs":
    case "r2":
    case "postgresql":
    case "snowflake":
    case "ducklake":
      return ["duckdb"];
    default:
      return ["duckdb"];
  }
}

export function defaultDriver(connectorType: ConnectorType): Driver {
  return supportedDrivers(connectorType)[0];
}

export interface ConnectorConfig {
  path?: string | null;
  format?: string | null;
  bucket?: string | null;
  region?: string | null;
  endpoint?: string | null;
  prefix?: string | null;
  account_id?: string | null;
  host?: string | null;
  port?: number | null;
  database?: string | null;
  user?: string | null;
  /** Snowflake warehouse */
  warehouse?: string | null;
  /** DuckLake catalog type: "duckdb", "postgres", or "sqlite" */
  catalog_type?: string | null;
  /** DuckLake metadata connection string */
  metadata_path?: string | null;
  /** DuckLake data file storage path */
  data_path?: string | null;
  /** DuckLake read-only mode */
  read_only?: boolean | null;
}

export interface Connector {
  id: string;
  name: string;
  connector_type: ConnectorType;
  config: ConnectorConfig;
  alias?: string | null;
}

export interface CatalogEntry {
  schema: string | null;
  name: string;
  entry_type: "table" | "view" | "file";
  columns: ColumnInfo[];
  row_count: number | null;
}

export interface DataSource {
  id: string;
  name: string;
  connector_id: string;
  qualified_name: string;
  view_name?: string | null;
  schema: ColumnInfo[];
  row_count: number | null;
  kind?: string;
  primary_key_column?: string | null;
  driver?: Driver;
}

/** @deprecated Use Connector instead — kept for backward compatibility in cloud_connector commands */
export interface ConnectionInfo {
  id: string;
  name: string;
  connector_type: ConnectorType;
  config: ConnectorConfig;
  alias?: string | null;
}

export interface FilePreview {
  format: string;
  schema: ColumnInfo[];
  row_count: number | null;
  preview_data: number[];
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
}

export interface AppError {
  error: true;
  message: string;
  code: "FILE_ERROR" | "QUERY_ERROR" | "AUTH_ERROR" | "EXPORT_ERROR" | "CONNECTOR_ERROR" | "GRAPH_ERROR";
}

export interface VertexTableDef {
  table_name: string;
  key_column?: string | null;
  label?: string | null;
}

export interface EdgeTableDef {
  table_name: string;
  source_key: string;
  source_vertex_table: string;
  source_vertex_key: string;
  destination_key: string;
  destination_vertex_table: string;
  destination_vertex_key: string;
  label?: string | null;
}

export interface PropertyGraphInfo {
  name: string;
  vertex_tables: string[];
  edge_tables: string[];
}

export type GraphAlgorithm = "pagerank" | "local_clustering_coefficient" | "weakly_connected_component";

export const GRAPH_ALGORITHMS: { value: GraphAlgorithm; label: string }[] = [
  { value: "pagerank", label: "PageRank" },
  { value: "local_clustering_coefficient", label: "Local Clustering Coefficient" },
  { value: "weakly_connected_component", label: "Weakly Connected Component" },
];

export interface PaginationState {
  page: number;
  pageSize: number;
  totalRows: number | null;
}

export interface Settings {
  sidebar_width: number;
  default_page_size: number;
  max_rows_per_query: number;
  default_export_format: string;
  streaming_enabled: boolean;
  streaming_threshold: number | null;
}

export type PersistedTab =
  | { id: string; type: "data"; dataSourceId: string; viewMode?: string }
  | { id: string; type: "query"; name: string; initialSql?: string; autoExecute?: boolean };

export interface PersistedWorkspace {
  openTabs: PersistedTab[];
  activeTabId: string | null;
}

// ──── ETL types ────

export type SyncStrategy = "full" | "incremental" | "append";

export const SYNC_STRATEGIES: { value: SyncStrategy; label: string; description: string }[] = [
  { value: "full", label: "Full Refresh", description: "Drop and recreate every run" },
  { value: "incremental", label: "Incremental", description: "Append only new/updated rows using a replication key" },
  { value: "append", label: "Append", description: "Always insert all rows (no dedup)" },
];

export type JobStatus = "idle" | "running" | "completed" | "failed" | "cancelled" | "partial";

export type TableStatus = "pending" | "running" | "completed" | "skipped" | "failed";

export interface TableSyncState {
  schema_name: string;
  table_name: string;
  status: TableStatus;
  rows_synced: number | null;
  error: string | null;
  started_at: string | null;
  completed_at: string | null;
  replication_key: string | null;
  replication_value: string | null;
}

export interface EtlJob {
  id: string;
  name: string;
  source_connector_id: string;
  target_connector_id: string;
  strategy: SyncStrategy;
  include_schemas: string[] | null;
  exclude_tables: string[] | null;
  skip_views: boolean;
  batch_size: number | null;
  status: JobStatus;
  table_states: TableSyncState[];
  created_at: string;
  last_run_at: string | null;
  last_completed_at: string | null;
  total_rows_synced: number;
  run_count: number;
}

export interface EtlProgressEvent {
  job_id: string;
  phase: string;
  current_table_index: number;
  total_tables: number;
  schema_name: string;
  table_name: string;
  status: "running" | "done" | "failed" | "skipped";
  rows_synced: number | null;
  error: string | null;
  elapsed_ms: number;
}

export interface EtlCompleteEvent {
  job_id: string;
  status: string;
  tables_migrated: number;
  tables_failed: number;
  total_rows: number;
  elapsed_ms: number;
}
