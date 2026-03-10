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
  | "r2";

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
  code: "FILE_ERROR" | "QUERY_ERROR" | "AUTH_ERROR" | "EXPORT_ERROR" | "CONNECTOR_ERROR";
}

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
