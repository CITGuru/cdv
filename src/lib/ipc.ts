import { invoke } from "@tauri-apps/api/core";
import type { DatasetInfo, ColumnInfo, S3Config } from "./types";

export async function registerDataset(path: string): Promise<DatasetInfo> {
  return invoke("register_dataset", { path });
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

export async function connectS3(config: S3Config): Promise<void> {
  return invoke("connect_s3", { config });
}

export async function listBucketFiles(
  bucket: string,
  prefix?: string
): Promise<string[]> {
  return invoke("list_bucket_files", { bucket, prefix });
}

export async function openRemoteDataset(
  s3Path: string
): Promise<DatasetInfo> {
  return invoke("open_remote_dataset", { s3Path });
}

export async function exportData(
  query: string,
  format: string,
  outputPath: string
): Promise<void> {
  return invoke("export_data", { query, format, outputPath });
}
