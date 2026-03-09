export interface ColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
  /** DuckDB key column: e.g. "PRI", "UNI", or empty */
  key?: string | null;
}

export interface DataSource {
  id: string;
  name: string;
  view_name: string;
  path: string;
  source_type: string;
  format: string;
  schema: ColumnInfo[];
  row_count: number | null;
  connection_id: string | null;
  /** "view" = view over file; "table" = materialized */
  kind?: string;
  /** User-chosen primary key column name (metadata) */
  primary_key_column?: string | null;
}

export interface ConnectionInfo {
  id: string;
  name: string;
  provider: string;
  endpoint: string | null;
  bucket: string;
  region: string;
  prefix: string | null;
  account_id?: string | null;
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
  code: "FILE_ERROR" | "QUERY_ERROR" | "AUTH_ERROR" | "EXPORT_ERROR";
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

/** Persisted tab (data or query) – matches backend PersistedTab */
export type PersistedTab =
  | { id: string; type: "data"; dataSourceId: string; viewMode?: string }
  | { id: string; type: "query"; name: string; initialSql?: string; autoExecute?: boolean };

export interface PersistedWorkspace {
  openTabs: PersistedTab[];
  activeTabId: string | null;
}
