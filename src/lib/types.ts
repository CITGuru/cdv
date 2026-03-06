export interface ColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
}

export interface DatasetInfo {
  id: string;
  name: string;
  path: string;
  source_type: string;
  format: string;
  schema: ColumnInfo[];
  row_count: number | null;
  duckdb_ref: string;
}

export interface S3Config {
  endpoint?: string;
  bucket: string;
  region: string;
  access_key: string;
  secret_key: string;
  prefix?: string;
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
