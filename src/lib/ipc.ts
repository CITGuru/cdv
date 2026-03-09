import { invoke } from "@tauri-apps/api/core";
import type {
  DataSource,
  ColumnInfo,
  ConnectionInfo,
  FilePreview,
  Settings,
  PersistedWorkspace,
} from "./types";

export async function previewFile(
  path: string,
  format?: string
): Promise<FilePreview> {
  return invoke("preview_file", { path, format: format ?? null });
}

export async function createDataSource(params: {
  name: string;
  viewName: string;
  path: string;
  format?: string;
  connectionId?: string;
  materialize?: boolean;
  primaryKeyColumn?: string | null;
}): Promise<DataSource> {
  return invoke("create_data_source", {
    name: params.name,
    viewName: params.viewName,
    path: params.path,
    format: params.format ?? null,
    connectionId: params.connectionId ?? null,
    materialize: params.materialize ?? null,
    primaryKeyColumn: params.primaryKeyColumn ?? null,
  });
}

export async function removeDataSource(id: string): Promise<void> {
  return invoke("remove_data_source", { id });
}

export async function updateDataSource(
  id: string,
  params: {
    path: string;
    name?: string;
    viewName?: string;
    format?: string;
    connectionId?: string;
  }
): Promise<DataSource> {
  return invoke("update_data_source", {
    id,
    path: params.path,
    name: params.name ?? null,
    view_name: params.viewName ?? null,
    format: params.format ?? null,
    connection_id: params.connectionId ?? null,
  });
}

export async function listDataSources(): Promise<DataSource[]> {
  return invoke("list_data_sources");
}

export async function getSchema(datasetId: string): Promise<ColumnInfo[]> {
  return invoke("get_schema", { datasetId });
}

export async function getPreview(datasetId: string): Promise<number[]> {
  return invoke("get_preview", { datasetId });
}

export async function runQuery(sql: string): Promise<number[]> {
  return invoke("run_query", { sql });
}

export async function runPaginatedQuery(
  sql: string,
  page: number,
  pageSize: number
): Promise<number[]> {
  return invoke("run_paginated_query", { sql, page, pageSize });
}

export async function streamQuery(sql: string): Promise<void> {
  return invoke("stream_query", { sql });
}

export type ConnectionProvider = "s3" | "gcp" | "cloudflare";

export async function createConnection(params: {
  name: string;
  provider: ConnectionProvider;
  endpoint?: string;
  bucket: string;
  region: string;
  accessKey: string;
  secretKey: string;
  prefix?: string;
  accountId?: string; // Cloudflare R2
}): Promise<ConnectionInfo> {
  return invoke("create_connection", {
    name: params.name,
    provider: params.provider,
    endpoint: params.endpoint ?? null,
    bucket: params.bucket,
    region: params.region,
    accessKey: params.accessKey,
    secretKey: params.secretKey,
    prefix: params.prefix ?? null,
    accountId: params.accountId ?? null,
  });
}

export async function removeConnection(id: string): Promise<void> {
  return invoke("remove_connection", { id });
}

export async function listConnections(): Promise<ConnectionInfo[]> {
  return invoke("list_connections");
}

export async function listConnectionFiles(
  connectionId: string,
  prefixOverride?: string
): Promise<string[]> {
  return invoke("list_connection_files", {
    connectionId,
    prefixOverride: prefixOverride ?? null,
  });
}

export async function exportData(
  query: string,
  format: string,
  outputPath: string
): Promise<void> {
  return invoke("export_data", { query, format, outputPath });
}

export async function getSettings(): Promise<Settings> {
  return invoke("get_settings");
}

export async function setSettings(settings: Settings): Promise<void> {
  return invoke("set_settings", { settings });
}

export async function getPersistedTabs(): Promise<PersistedWorkspace> {
  return invoke("get_persisted_tabs");
}

export async function setPersistedTabs(workspace: PersistedWorkspace): Promise<void> {
  return invoke("set_persisted_tabs", { workspace });
}
