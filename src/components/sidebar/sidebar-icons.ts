import type { ComponentType, SVGProps } from "react";
import {
  FileSpreadsheet,
  FileText,
  FileJson,
  Table2,
} from "lucide-react";
import {
  SqliteIcon,
  DuckDbIcon,
  PostgresqlIcon,
  SnowflakeIcon,
  AmazonS3Icon,
  GoogleCloudStorageIcon,
  CloudflareIcon,
  ParquetIcon,
  AvroIcon,
} from "@/components/icons/ServiceIcons";
import type { DataSource, Connector } from "@/lib/types";

export type IconComponent = ComponentType<SVGProps<SVGSVGElement> & { className?: string }>;

export function getDataSourceIcon(_source: DataSource, connector?: Connector): IconComponent {
  if (!connector) return FileSpreadsheet;
  switch (connector.connector_type) {
    case "sqlite":
      return SqliteIcon;
    case "duckdb":
      return DuckDbIcon;
    case "postgresql":
      return PostgresqlIcon;
    case "s3":
      return AmazonS3Icon;
    case "gcs":
      return GoogleCloudStorageIcon;
    case "r2":
      return CloudflareIcon;
    default: {
      const fmt = (connector.config.format ?? "").toLowerCase();
      switch (fmt) {
        case "csv":
        case "tsv":
          return FileText;
        case "json":
        case "jsonl":
          return FileJson;
        case "parquet":
          return ParquetIcon;
        case "avro":
          return AvroIcon;
        case "xlsx":
          return FileSpreadsheet;
        case "arrow_ipc":
        case "arrow":
          return Table2;
        default:
          return FileSpreadsheet;
      }
    }
  }
}

export function getConnectorIcon(connector: Connector): IconComponent {
  switch (connector.connector_type) {
    case "sqlite":
      return SqliteIcon;
    case "duckdb":
      return DuckDbIcon;
    case "postgresql":
      return PostgresqlIcon;
    case "snowflake":
      return SnowflakeIcon;
    case "ducklake":
      return DuckDbIcon;
    case "s3":
      return AmazonS3Icon;
    case "gcs":
      return GoogleCloudStorageIcon;
    case "r2":
      return CloudflareIcon;
    default:
      return FileSpreadsheet;
  }
}

export function getConnectorLabel(connector: Connector): string {
  switch (connector.connector_type) {
    case "sqlite":
      return "SQLite";
    case "duckdb":
      return "DuckDB";
    case "postgresql":
      return "PostgreSQL";
    case "snowflake":
      return "Snowflake";
    case "ducklake":
      return "DuckLake";
    case "s3":
      return "S3";
    case "gcs":
      return "GCS";
    case "r2":
      return "R2";
    default:
      return "";
  }
}

export function getConnectorIconColor(connector: Connector): string {
  switch (connector.connector_type) {
    case "sqlite":
      return "text-sky-500";
    case "duckdb":
      return "text-amber-600";
    case "postgresql":
      return "text-[#4169E1]";
    case "snowflake":
      return "text-cyan-400";
    case "ducklake":
      return "text-emerald-600";
    case "s3":
      return "text-amber-500";
    case "gcs":
      return "text-emerald-500";
    case "r2":
      return "text-orange-500";
    default:
      return "text-muted-foreground";
  }
}

export function getFileIconColor(connector?: Connector): string {
  if (!connector) return "text-muted-foreground";
  const fmt = (connector.config.format ?? "").toLowerCase();
  switch (fmt) {
    case "csv":
    case "tsv":
      return "text-green-500";
    case "json":
    case "jsonl":
      return "text-yellow-500";
    case "parquet":
      return "text-purple-500";
    case "xlsx":
      return "text-emerald-600";
    case "arrow_ipc":
    case "arrow":
      return "text-rose-500";
    default:
      return "text-muted-foreground";
  }
}
