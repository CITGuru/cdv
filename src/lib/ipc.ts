import { invoke } from "@tauri-apps/api/core";
import type {
  DataSource,
  ColumnInfo,
  Connector,
  ConnectorType,
  ConnectorConfig,
  CatalogEntry,
  ConnectorBrowseCache,
  FilePreview,
  Settings,
  PersistedWorkspace,
  Driver,
  VertexTableDef,
  EdgeTableDef,
  PropertyGraphInfo,
  GraphAlgorithm,
  EtlJob,
  SyncStrategy,
} from "./types";

// ──── File preview ────

export async function previewFile(
  path: string,
  format?: string
): Promise<FilePreview> {
  return invoke("preview_file", { path, format: format ?? null });
}

// ──── Data sources ────

export async function createDataSource(params: {
  name: string;
  viewName: string;
  connectorId: string;
  materialize?: boolean;
  primaryKeyColumn?: string | null;
  dbSchema?: string;
  dbTable?: string;
  /** Postgres / Snowflake database name when importing from a non-default attached DB */
  dbDatabase?: string | null;
  driver?: Driver;
}): Promise<DataSource> {
  return invoke("create_data_source", {
    name: params.name,
    viewName: params.viewName,
    connectorId: params.connectorId,
    materialize: params.materialize ?? null,
    primaryKeyColumn: params.primaryKeyColumn ?? null,
    dbSchema: params.dbSchema ?? null,
    dbTable: params.dbTable ?? null,
    dbDatabase: params.dbDatabase ?? null,
    driver: params.driver ?? null,
  });
}

export async function removeDataSource(id: string): Promise<void> {
  return invoke("remove_data_source", { id });
}

export async function updateDataSource(
  id: string,
  params: {
    name?: string;
    viewName?: string;
  }
): Promise<DataSource> {
  return invoke("update_data_source", {
    id,
    name: params.name ?? null,
    viewName: params.viewName ?? null,
  });
}

export async function refreshDataSource(id: string): Promise<DataSource> {
  return invoke("refresh_data_source", { id });
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

// ──── Query engine ────

export async function runQuery(sql: string): Promise<number[]> {
  return invoke("run_query", { sql });
}

export async function runPaginatedQuery(
  sql: string,
  page: number,
  pageSize: number
): Promise<number[]> {
  return invoke("run_paginated_query", { args: { sql, page, pageSize } });
}

export async function streamQuery(sql: string): Promise<void> {
  return invoke("stream_query", { sql });
}

// ──── Connectors ────

export async function addConnector(params: {
  name: string;
  connectorType: ConnectorType;
  config: ConnectorConfig;
  accessKey?: string;
  secretKey?: string;
}): Promise<Connector> {
  return invoke("add_connector", {
    name: params.name,
    connectorType: params.connectorType,
    config: params.config,
    accessKey: params.accessKey ?? null,
    secretKey: params.secretKey ?? null,
  });
}

export async function removeConnector(id: string): Promise<void> {
  return invoke("remove_connector", { id });
}

export async function testConnector(params: {
  connectorType: ConnectorType;
  config: ConnectorConfig;
  accessKey?: string;
  secretKey?: string;
}): Promise<void> {
  return invoke("test_connector", {
    connectorType: params.connectorType,
    config: params.config,
    accessKey: params.accessKey ?? null,
    secretKey: params.secretKey ?? null,
  });
}

export async function introspectConnector(id: string): Promise<CatalogEntry[]> {
  return invoke("introspect_connector", { id });
}

export async function listConnectors(): Promise<Connector[]> {
  return invoke("list_connectors");
}

export async function getCachedCatalogs(): Promise<
  Record<string, ConnectorBrowseCache>
> {
  return invoke("get_cached_catalogs");
}

export async function listConnectorDatabases(
  connectorId: string,
  options?: { refresh?: boolean }
): Promise<string[]> {
  return invoke("list_connector_databases", {
    id: connectorId,
    refresh: options?.refresh ?? null,
  });
}

export async function connectConnectorDatabase(
  connectorId: string,
  database: string
): Promise<CatalogEntry[]> {
  return invoke("connect_connector_database", {
    id: connectorId,
    database,
  });
}

export async function listPgDatabases(params: {
  config: ConnectorConfig;
  secretKey?: string;
}): Promise<string[]> {
  return invoke("list_pg_databases", {
    config: params.config,
    secretKey: params.secretKey ?? null,
  });
}

// ──── Cloud connections (legacy compatibility wrappers) ────

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
  accountId?: string;
}): Promise<Connector> {
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

export async function listConnections(): Promise<Connector[]> {
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

// ──── Download ────

export async function downloadUrl(url: string): Promise<string> {
  return invoke("download_url", { url });
}

// ──── Export ────

export async function exportData(
  query: string,
  format: string,
  outputPath: string
): Promise<void> {
  return invoke("export_data", { query, format, outputPath });
}

// ──── Settings ────

export async function getSettings(): Promise<Settings> {
  return invoke("get_settings");
}

export async function setSettings(settings: Settings): Promise<void> {
  return invoke("set_settings", { settings });
}

// ──── Graph (DuckPGQ) ────

export async function checkGraphSupport(): Promise<boolean> {
  return invoke("check_graph_support");
}

export async function installGraphExtension(): Promise<void> {
  return invoke("install_graph_extension");
}

export async function createPropertyGraph(
  name: string,
  vertexTables: VertexTableDef[],
  edgeTables: EdgeTableDef[]
): Promise<void> {
  return invoke("create_property_graph", { name, vertexTables, edgeTables });
}

export async function listPropertyGraphs(): Promise<PropertyGraphInfo[]> {
  return invoke("list_property_graphs");
}

export async function dropPropertyGraph(name: string): Promise<void> {
  return invoke("drop_property_graph", { name });
}

export async function getPropertyGraphInfo(name: string): Promise<PropertyGraphInfo> {
  return invoke("get_property_graph_info", { name });
}

export async function runGraphAlgorithm(
  graphName: string,
  algorithm: GraphAlgorithm,
  vertexLabel: string,
  edgeLabel: string
): Promise<number[]> {
  return invoke("run_graph_algorithm", { graphName, algorithm, vertexLabel, edgeLabel });
}

// ──── Workspace ────

export async function getPersistedTabs(): Promise<PersistedWorkspace> {
  return invoke("get_persisted_tabs");
}

export async function setPersistedTabs(workspace: PersistedWorkspace): Promise<void> {
  return invoke("set_persisted_tabs", { workspace });
}

// ──── ETL Jobs ────

export async function createEtlJob(params: {
  name: string;
  sourceConnectorId: string;
  targetConnectorId: string;
  strategy: SyncStrategy;
  includeSchemas?: string[];
  excludeTables?: string[];
  skipViews?: boolean;
  batchSize?: number;
}): Promise<EtlJob> {
  return invoke("create_etl_job", {
    name: params.name,
    sourceConnectorId: params.sourceConnectorId,
    targetConnectorId: params.targetConnectorId,
    strategy: params.strategy,
    includeSchemas: params.includeSchemas ?? null,
    excludeTables: params.excludeTables ?? null,
    skipViews: params.skipViews ?? null,
    batchSize: params.batchSize ?? null,
  });
}

export async function listEtlJobs(): Promise<EtlJob[]> {
  return invoke("list_etl_jobs");
}

export async function getEtlJob(jobId: string): Promise<EtlJob> {
  return invoke("get_etl_job", { jobId });
}

export async function deleteEtlJob(jobId: string): Promise<void> {
  return invoke("delete_etl_job", { jobId });
}

export async function runEtlJob(jobId: string): Promise<void> {
  return invoke("run_etl_job", { jobId });
}

export async function cancelEtlJob(): Promise<void> {
  return invoke("cancel_etl_job");
}

export async function previewEtlJob(params: {
  sourceConnectorId: string;
  includeSchemas?: string[];
  excludeTables?: string[];
  skipViews?: boolean;
}): Promise<CatalogEntry[]> {
  return invoke("preview_etl_job", {
    sourceConnectorId: params.sourceConnectorId,
    includeSchemas: params.includeSchemas ?? null,
    excludeTables: params.excludeTables ?? null,
    skipViews: params.skipViews ?? null,
  });
}
