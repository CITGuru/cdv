import { useState, useEffect } from "react";
import { useForm, type UseFormReturn } from "react-hook-form";
import { open } from "@tauri-apps/plugin-dialog";
import {
  Loader2,
  FolderOpen,
  FileSpreadsheet,
  Cloud,
  Database,
  ArrowLeft,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Table2,
  Copy,
  Info,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type {
  DataSource,
  Connector,
  ConnectorType,
  ConnectorConfig,
  CatalogEntry,
  FilePreview,
  Driver,
} from "@/lib/types";
import { DRIVER_OPTIONS, supportedDrivers, defaultDriver } from "@/lib/types";
import type { ParsedError } from "@/lib/errors";
import { extractError } from "@/lib/errors";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";
import {
  previewFile,
  createDataSource,
  updateDataSource,
  listConnectionFiles,
  introspectConnector,
  downloadUrl,
} from "@/lib/ipc";

// ──── Form types ────

type AuthMode = "none" | "user_password";

interface FileConnectionFields {
  path: string;
  pathType: "local" | "url";
  url: string;
  format: string;
  downloadLocal: boolean;
}

interface HostConnectionFields {
  host: string;
  port: string;
  database: string;
  authMode: AuthMode;
  auth: { user: string; password: string; [key: string]: string };
  warehouse: string;
}

interface DataSourceFormValues {
  name: string;
  comment: string;
  sourceType: SourceKind;
  driver: Driver;
  file: FileConnectionFields;
  db: HostConnectionFields;
  cloud: { connectionId: string };
  viewName: string;
  materialize: boolean;
  selectedPkColumn: string | null;
}

const FORM_DEFAULTS: DataSourceFormValues = {
  name: "",
  comment: "",
  sourceType: "columnar",
  driver: "duckdb",
  file: { path: "", pathType: "local", url: "", format: "csv", downloadLocal: false },
  db: {
    host: "localhost",
    port: "5432",
    database: "",
    authMode: "user_password",
    auth: { user: "", password: "" },
    warehouse: "",
  },
  cloud: { connectionId: "" },
  viewName: "",
  materialize: false,
  selectedPkColumn: null,
};

// ──── Modal types ────

interface AddDataSourceModalProps {
  open: boolean;
  onClose: () => void;
  onCreated: (ds: DataSource, opts?: { openTab?: boolean }) => void;
  connectors: Connector[];
  onAddConnector?: (params: {
    name: string;
    connectorType: ConnectorType;
    config: ConnectorConfig;
    accessKey?: string;
    secretKey?: string;
  }) => Promise<Connector>;
  onTestConnector?: (params: {
    connectorType: ConnectorType;
    config: ConnectorConfig;
    accessKey?: string;
    secretKey?: string;
  }) => Promise<void>;
  initialFilePath?: string;
  initialUrl?: string;
  onOpenNewConnection?: () => void;
  existingDataSource?: DataSource | null;
  onUpdated?: (ds: DataSource) => void;
}

type Step = "source" | "configure" | "select-tables";

export type SourceKind =
  | "columnar"
  | "sqlite"
  | "duckdb"
  | "postgresql"
  | "snowflake"
  | "connection";

const SOURCE_TYPE_OPTIONS: { value: SourceKind; label: string }[] = [
  { value: "columnar", label: "Columnar (CSV, Parquet, Arrow, Excel)" },
  { value: "sqlite", label: "SQLite" },
  { value: "duckdb", label: "DuckDB" },
  { value: "postgresql", label: "PostgreSQL" },
  { value: "snowflake", label: "Snowflake" },
  { value: "connection", label: "Cloud (S3 / GCS / R2)" },
];

const FORMATS = [
  { value: "csv", label: "CSV" },
  { value: "tsv", label: "TSV" },
  { value: "json", label: "JSON" },
  { value: "jsonl", label: "JSONL" },
  { value: "parquet", label: "Parquet" },
  { value: "xlsx", label: "Excel" },
  { value: "avro", label: "Avro" },
  { value: "arrow_ipc", label: "Arrow IPC" },
];

function sourceKindToConnectorType(sk: SourceKind): ConnectorType {
  switch (sk) {
    case "columnar": return "local_file";
    case "sqlite": return "sqlite";
    case "duckdb": return "duckdb";
    case "postgresql": return "postgresql";
    case "snowflake": return "snowflake";
    case "connection": return "s3";
    default: return "local_file";
  }
}

function fileBaseName(filePath: string): string {
  return filePath.split(/[/\\]/).pop() ?? "Untitled";
}

const EXT_TO_FORMAT: Record<string, string> = {
  csv: "csv",
  tsv: "tsv",
  json: "json",
  jsonl: "jsonl",
  parquet: "parquet",
  xlsx: "xlsx",
  avro: "avro",
  arrow: "arrow_ipc",
  ipc: "arrow_ipc",
};

function pathToFormat(pathOrUrl: string): string {
  const path = pathOrUrl.includes("?") ? pathOrUrl.split("?")[0] : pathOrUrl;
  const ext = (path.split(/[/\\]/).pop() ?? "").replace(/^.*\./, "").toLowerCase();
  return EXT_TO_FORMAT[ext] ?? "csv";
}

function nameToViewName(name: string): string {
  return name
    .replace(/\.[^.]+$/, "")
    .replace(/[^a-zA-Z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, "")
    .toLowerCase();
}

// ──── Main component ────

export function AddDataSourceModal({
  open: isOpen,
  onClose,
  onCreated,
  connectors,
  onAddConnector,
  onTestConnector,
  initialFilePath,
  initialUrl,
  onOpenNewConnection,
  existingDataSource,
  onUpdated,
}: AddDataSourceModalProps) {
  const isUpdateMode = !!existingDataSource;

  const form = useForm<DataSourceFormValues>({ defaultValues: FORM_DEFAULTS });
  const sourceType = form.watch("sourceType");
  const filePath = form.watch("file.path");
  const fileUrl = form.watch("file.url");
  const dbHost = form.watch("db.host");
  const dbDatabase = form.watch("db.database");
  const driver = form.watch("driver");

  const [step, setStep] = useState<Step>("source");
  const [connectionFiles, setConnectionFiles] = useState<string[]>([]);
  const [loadingFiles, setLoadingFiles] = useState(false);
  const [preview, setPreview] = useState<FilePreview | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ParsedError | null>(null);
  const [creating, setCreating] = useState(false);
  const [fileConnectorId, setFileConnectorId] = useState<string | null>(null);
  const [dbConnectorId, setDbConnectorId] = useState<string | null>(null);
  const [dbCatalog, setDbCatalog] = useState<CatalogEntry[]>([]);
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());
  const [testing, setTesting] = useState(false);
  const [testSuccess, setTestSuccess] = useState(false);
  const [testResultText, setTestResultText] = useState<string | null>(null);

  const cloudConnectors = connectors.filter((c) =>
    ["s3", "gcs", "r2"].includes(c.connector_type)
  );

  const connectorType = sourceKindToConnectorType(sourceType);
  const availableDrivers = supportedDrivers(connectorType);
  const driverLabel = DRIVER_OPTIONS.find((d) => d.value === driver)?.label ?? "DuckDB";

  // ──── Reset on close ────

  useEffect(() => {
    if (!isOpen) {
      form.reset(FORM_DEFAULTS);
      setStep("source");
      setPreview(null);
      setError(null);
      setCreating(false);
      setLoading(false);
      setFileConnectorId(null);
      setDbConnectorId(null);
      setDbCatalog([]);
      setSelectedTables(new Set());
      setTesting(false);
      setTestSuccess(false);
      setTestResultText(null);
      setConnectionFiles([]);
      setLoadingFiles(false);
    }
  }, [isOpen]);

  // ──── Reset test status when db fields change ────

  const dbAuthUser = form.watch("db.auth.user");
  const dbAuthPassword = form.watch("db.auth.password");
  const dbPort = form.watch("db.port");

  useEffect(() => {
    setTestSuccess(false);
  }, [dbHost, dbPort, dbDatabase, dbAuthUser, dbAuthPassword]);

  // ──── Initialize from existingDataSource ────

  useEffect(() => {
    if (isOpen && existingDataSource) {
      const conn = connectors.find((c) => c.id === existingDataSource.connector_id);
      if (conn?.connector_type === "local_file") {
        form.setValue("file.path", conn.config.path ?? "");
        form.setValue("name", existingDataSource.name);
        form.setValue("viewName", existingDataSource.view_name ?? "");
        form.setValue("file.format", conn.config.format ?? "");
        form.setValue("sourceType", "columnar");
        loadPreview(conn.config.path ?? "", conn.config.format ?? undefined);
      }
    }
  }, [isOpen, existingDataSource?.id]);

  // ──── Initialize from initialFilePath ────

  useEffect(() => {
    if (isOpen && initialFilePath && !existingDataSource) {
      const ext = (initialFilePath.match(/\.([^.]+)$/)?.[1] ?? "").toLowerCase();
      if (ext === "duckdb") {
        form.setValue("file.path", initialFilePath);
        const base = fileBaseName(initialFilePath).replace(/\.[^.]+$/, "");
        form.setValue("name", base, { shouldDirty: true });
        form.setValue("sourceType", "duckdb");
      } else if (["db", "sqlite", "sqlite3"].includes(ext)) {
        form.setValue("file.path", initialFilePath);
        const base = fileBaseName(initialFilePath).replace(/\.[^.]+$/, "");
        form.setValue("name", base, { shouldDirty: true });
        form.setValue("sourceType", "sqlite");
      } else {
        form.setValue("file.path", initialFilePath);
        form.setValue("sourceType", "columnar");
        loadPreview(initialFilePath);
      }
    }
  }, [isOpen, initialFilePath, existingDataSource]);

  // ──── Initialize from initialUrl ────

  useEffect(() => {
    if (isOpen && initialUrl && !existingDataSource) {
      form.setValue("file.url", initialUrl);
      form.setValue("sourceType", "columnar");
      const pathname = initialUrl.split("?")[0];
      const ext = (pathname.match(/\.([^.]+)$/)?.[1] ?? "").toLowerCase();
      const map: Record<string, string> = {
        csv: "csv", tsv: "tsv", json: "json", jsonl: "jsonl",
        parquet: "parquet", xlsx: "xlsx", avro: "avro",
      };
      form.setValue("file.format", map[ext] ?? "csv");
    }
  }, [isOpen, initialUrl, existingDataSource]);

  // ──── Source type change handler (called from UI, not an effect) ────

  const handleSourceTypeChange = (newType: SourceKind) => {
    form.setValue("file", { ...FORM_DEFAULTS.file });
    form.setValue("db", { ...FORM_DEFAULTS.db });
    form.setValue("cloud", { ...FORM_DEFAULTS.cloud });
    form.setValue("sourceType", newType);
    if (newType === "snowflake") form.setValue("db.port", "443");
    setPreview(null);
    setError(null);
    setStep("source");
    setTestSuccess(false);
    setTestResultText(null);
    setConnectionFiles([]);
    setFileConnectorId(null);
    setDbConnectorId(null);
    setDbCatalog([]);
    setSelectedTables(new Set());
    form.setValue("driver", defaultDriver(sourceKindToConnectorType(newType)));
  };

  // ──── Preview ────

  const loadPreview = async (path: string, format?: string) => {
    setLoading(true);
    setError(null);
    try {
      const result = await previewFile(path, format);
      setPreview(result);
      const idCol = result.schema.find(
        (c: { name: string }) => c.name.toLowerCase() === "id"
      );
      form.setValue("selectedPkColumn", idCol ? idCol.name : null);
      const currentName = (form.getValues("name") ?? "").trim();
      if (!currentName) {
        const baseName = fileBaseName(path).replace(/\.[^.]+$/, "");
        form.setValue("name", baseName, { shouldDirty: true });
      }
      const derivedName = (form.getValues("name") ?? "").trim();
      form.setValue("viewName", nameToViewName(derivedName), { shouldDirty: true });
      form.setValue("file.format", result.format);
      setStep("configure");
    } catch (e) {
      setError(extractError(e));
    } finally {
      setLoading(false);
    }
  };

  // ──── File pick handlers ────

  const handlePickFile = async () => {
    const result = await open({
      multiple: false,
      filters: [
        {
          name: "Data Files",
          extensions: [
            "csv", "tsv", "json", "jsonl", "parquet",
            "xlsx", "avro", "arrow", "ipc",
          ],
        },
      ],
    });
    if (result) {
      form.setValue("file.path", result);
      form.setValue("file.format", pathToFormat(result), { shouldDirty: true });
      const currentName = (form.getValues("name") ?? "").trim();
      if (!currentName) {
        const baseName = fileBaseName(result).replace(/\.[^.]+$/, "");
        form.setValue("name", baseName, { shouldDirty: true });
      }
    }
  };

  const handleLoadFromUrl = async () => {
    const u = form.getValues("file.url").trim();
    if (!u) return;
    const wantDownload = form.getValues("file.downloadLocal");
    const fmt = pathToFormat(u);
    form.setValue("file.format", fmt, { shouldDirty: true });

    if (wantDownload) {
      setLoading(true);
      setError(null);
      try {
        const localPath = await downloadUrl(u);
        form.setValue("file.path", localPath);
        form.setValue("file.pathType", "local");
        await loadPreview(localPath, fmt);
      } catch (err) {
        setError(extractError(err));
      } finally {
        setLoading(false);
      }
    } else {
      form.setValue("file.path", u);
      form.setValue("file.pathType", "url");
      await loadPreview(u, fmt);
    }
  };

  const handlePickDatabaseFile = async () => {
    const result = await open({
      multiple: false,
      filters: [
        {
          name: "SQLite / DuckDB",
          extensions: ["db", "sqlite", "sqlite3", "duckdb"],
        },
      ],
    });
    if (result) {
      form.setValue("file.path", result);
      const base = fileBaseName(result).replace(/\.[^.]+$/, "");
      form.setValue("name", base, { shouldDirty: true });
      form.setValue("viewName", nameToViewName(base), { shouldDirty: true });
    }
  };

  // ──── Cloud connection handlers ────

  const handleSelectConnection = async (connId: string) => {
    form.setValue("cloud.connectionId", connId);
    setLoadingFiles(true);
    setError(null);
    try {
      const files = await listConnectionFiles(connId);
      setConnectionFiles(files);
    } catch (e) {
      setError(extractError(e));
    } finally {
      setLoadingFiles(false);
    }
  };

  const handleSelectRemoteFile = async (path: string) => {
    form.setValue("file.path", path);
    await loadPreview(path);
  };

  // ──── Format change ────

  const handleFormatChange = async (fmt: string) => {
    form.setValue("file.format", fmt);
    const fp = form.getValues("file.path");
    if (fp) {
      await loadPreview(fp, fmt);
    }
  };

  // ──── Create / import handlers ────

  const handleCreate = async () => {
    const { name, viewName, materialize, selectedPkColumn, driver: drv, file } = form.getValues();
    if (!file.path || !name || !viewName) return;
    setCreating(true);
    setError(null);
    try {
      if (isUpdateMode && existingDataSource && onUpdated) {
        const ds = await updateDataSource(existingDataSource.id, { name, viewName });
        onUpdated(ds);
      } else {
        let connId = fileConnectorId;
        if (!connId && onAddConnector) {
          const conn = await onAddConnector({
            name,
            connectorType: "local_file" as ConnectorType,
            config: {
              path: file.path,
              format: file.format || undefined,
            },
          });
          connId = conn.id;
          setFileConnectorId(connId);
        }
        if (!connId) return;
        const ds = await createDataSource({
          name, viewName, connectorId: connId,
          materialize, primaryKeyColumn: selectedPkColumn, driver: drv,
        });
        onCreated(ds);
      }
      onClose();
    } catch (e) {
      setError(extractError(e));
    } finally {
      setCreating(false);
    }
  };

  const handleCreateFromCloud = async () => {
    const { name, viewName, driver: drv, file, cloud } = form.getValues();
    if (!file.path || !name || !viewName || !cloud.connectionId) return;
    setCreating(true);
    setError(null);
    try {
      const ds = await createDataSource({
        name, viewName, connectorId: cloud.connectionId, driver: drv,
      });
      onCreated(ds);
      onClose();
    } catch (e) {
      setError(extractError(e));
    } finally {
      setCreating(false);
    }
  };

  // ──── Database connect handlers ────

  const handleConnectDatabase = async () => {
    const { file } = form.getValues();
    let { name } = form.getValues();
    const url = file.url?.trim();
    const path = file.path || url;
    if (!path) return;
    if (!name.trim()) {
      const base = fileBaseName(path).replace(/\.[^.]+$/, "").replace(/\?.*$/, "");
      name = base;
      form.setValue("name", base, { shouldDirty: true });
      form.setValue("viewName", nameToViewName(base), { shouldDirty: true });
    }
    const isDuckdb = sourceType === "duckdb" || path.toLowerCase().endsWith(".duckdb");
    setLoading(true);
    setError(null);
    try {
      if (onAddConnector) {
        const conn = await onAddConnector({
          name: name.trim(),
          connectorType: isDuckdb ? "duckdb" : "sqlite",
          config: { path },
        });
        setDbConnectorId(conn.id);
        const entries = await introspectConnector(conn.id);
        setDbCatalog(entries);
        setStep("select-tables");
      }
    } catch (e) {
      setError(extractError(e));
    } finally {
      setLoading(false);
    }
  };

  const getDbConfig = (): ConnectorConfig => {
    const { db } = form.getValues();
    return {
      host: db.host,
      port: parseInt(db.port) || 5432,
      database: db.database,
      user: db.authMode === "user_password" ? (db.auth.user || undefined) : undefined,
    };
  };

  const getDbSecretKey = (): string | undefined => {
    const { db } = form.getValues();
    return db.authMode === "user_password" ? (db.auth.password || undefined) : undefined;
  };

  const handleTestPostgres = async () => {
    setTesting(true);
    setError(null);
    setTestSuccess(false);
    setTestResultText(null);
    try {
      if (onTestConnector) {
        await onTestConnector({
          connectorType: "postgresql",
          config: getDbConfig(),
          secretKey: getDbSecretKey(),
        });
        setTestSuccess(true);
        const { db } = form.getValues();
        const url = `duckdb:postgresql://${db.host}:${db.port || "5432"}/${db.database}`;
        setTestResultText(
          ["DBMS: PostgreSQL", "Driver: PostgreSQL DuckDB Driver", `URL: ${url}`, "Connection: Succeeded"].join("\n")
        );
        setTimeout(() => { setTestSuccess(false); setTestResultText(null); }, 8000);
      }
    } catch (e) {
      setError(extractError(e));
    } finally {
      setTesting(false);
    }
  };

  const handleConnectPostgres = async () => {
    const { db } = form.getValues();
    let { name } = form.getValues();
    if (!db.host || !db.database) return;
    if (!name.trim()) {
      name = db.database;
      form.setValue("name", name, { shouldDirty: true });
      form.setValue("viewName", nameToViewName(name), { shouldDirty: true });
    }
    setLoading(true);
    setError(null);
    try {
      if (onAddConnector) {
        const conn = await onAddConnector({
          name: name.trim(),
          connectorType: "postgresql",
          config: getDbConfig(),
          secretKey: getDbSecretKey(),
        });
        setDbConnectorId(conn.id);
        const entries = await introspectConnector(conn.id);
        setDbCatalog(entries);
        setStep("select-tables");
      }
    } catch (e) {
      setError(extractError(e));
    } finally {
      setLoading(false);
    }
  };

  const handleTestSnowflake = async () => {
    setTesting(true);
    setError(null);
    setTestSuccess(false);
    setTestResultText(null);
    try {
      if (onTestConnector) {
        const { db } = form.getValues();
        await onTestConnector({
          connectorType: "snowflake",
          config: {
            host: db.host,
            port: parseInt(db.port) || 443,
            database: db.database,
            user: db.authMode === "user_password" ? (db.auth.user || undefined) : undefined,
            warehouse: db.warehouse || undefined,
          },
          secretKey: getDbSecretKey(),
        });
        setTestSuccess(true);
        setTestResultText(
          ["DBMS: Snowflake", "Driver: Snowflake DuckDB Driver", `URL: duckdb:snowflake://${db.host}:${db.port}`, "Connection: Succeeded"].join("\n")
        );
        setTimeout(() => { setTestSuccess(false); setTestResultText(null); }, 8000);
      }
    } catch (e) {
      setError(extractError(e));
    } finally {
      setTesting(false);
    }
  };

  const handleConnectSnowflake = async () => {
    const { db } = form.getValues();
    let { name } = form.getValues();
    if (!db.host || !db.database) return;
    if (!name.trim()) {
      name = db.database;
      form.setValue("name", name, { shouldDirty: true });
      form.setValue("viewName", nameToViewName(name), { shouldDirty: true });
    }
    setLoading(true);
    setError(null);
    try {
      if (onAddConnector) {
        const conn = await onAddConnector({
          name: name.trim(),
          connectorType: "snowflake",
          config: {
            host: db.host,
            port: parseInt(db.port) || 443,
            database: db.database,
            user: db.authMode === "user_password" ? (db.auth.user || undefined) : undefined,
            warehouse: db.warehouse || undefined,
          },
          secretKey: getDbSecretKey(),
        });
        setDbConnectorId(conn.id);
        const entries = await introspectConnector(conn.id);
        setDbCatalog(entries);
        setStep("select-tables");
      }
    } catch (e) {
      setError(extractError(e));
    } finally {
      setLoading(false);
    }
  };

  const handleCopyTestResult = async () => {
    if (testResultText && navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(testResultText);
    }
  };

  const handleTestDb = async () => {
    const fp = form.getValues("file.path") || form.getValues("file.url")?.trim();
    if (!fp) return;
    setTesting(true);
    setError(null);
    setTestSuccess(false);
    setTestResultText(null);
    const isDuckdb = sourceType === "duckdb" || fp.toLowerCase().endsWith(".duckdb");
    try {
      if (onTestConnector) {
        await onTestConnector({
          connectorType: isDuckdb ? "duckdb" : "sqlite",
          config: { path: fp },
        });
        setTestSuccess(true);
        setTestResultText(`DBMS: ${isDuckdb ? "DuckDB" : "SQLite"}\nFile: ${fp}\nConnection: Succeeded`);
        setTimeout(() => { setTestSuccess(false); setTestResultText(null); }, 8000);
      }
    } catch (e) {
      setError(extractError(e));
    } finally {
      setTesting(false);
    }
  };

  // ──── Table import ────

  const handleImportSelectedTables = async () => {
    if (!dbConnectorId || selectedTables.size === 0) return;
    const drv = form.getValues("driver");
    setCreating(true);
    setError(null);
    try {
      for (const tableKey of selectedTables) {
        const [schema, tableName] = tableKey.split(".");
        const ds = await createDataSource({
          name: tableName,
          viewName: tableName,
          connectorId: dbConnectorId,
          dbSchema: schema,
          dbTable: tableName,
          driver: drv,
        });
        onCreated(ds, { openTab: false });
      }
      onClose();
    } catch (e) {
      setError(extractError(e));
    } finally {
      setCreating(false);
    }
  };

  const toggleTable = (schema: string, tableName: string) => {
    const key = `${schema}.${tableName}`;
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  // ──── Computed values ────

  const canTest = (sourceType === "postgresql" || sourceType === "snowflake") && !!dbHost && !!dbDatabase;
  const canTestDb = (sourceType === "sqlite" || sourceType === "duckdb") && !!(filePath || fileUrl);

  const handleSourceConnect = () => {
    if (sourceType === "postgresql") handleConnectPostgres();
    else if (sourceType === "snowflake") handleConnectSnowflake();
    else if (sourceType === "sqlite" || sourceType === "duckdb") handleConnectDatabase();
    else if (sourceType === "columnar") {
      const url = form.getValues("file.url").trim();
      if (url) {
        handleLoadFromUrl();
      } else {
        const fp = form.getValues("file.path");
        if (fp) {
          const fmt = form.getValues("file.format");
          loadPreview(fp, fmt);
        }
      }
    }
  };

  const cloudConnectionId = form.watch("cloud.connectionId");

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="w-[95vw] max-w-3xl sm:max-w-3xl max-h-[90vh] flex flex-col p-0 gap-0 overflow-hidden">
        <DialogTitle className="sr-only">
          {isUpdateMode ? "Update Data Source" : "Data Sources and Drivers"}
        </DialogTitle>
        {/* Title bar */}
        <div className="text-center py-2.5 border-b border-border shrink-0">
          <span className="text-sm font-medium">
            {isUpdateMode ? "Update Data Source" : step === "select-tables" ? "Select Tables" : step === "configure" ? "Configure Data Source" : "Data Sources and Drivers"}
          </span>
        </div>

        {error && <div className="px-5 pt-3"><ErrorDisplay error={error} compact /></div>}

        {step === "source" ? (
          <SourceStep
            form={form}
            onSourceTypeChange={handleSourceTypeChange}
            onPickFile={handlePickFile}
            onPickDatabaseFile={handlePickDatabaseFile}
            cloudConnectors={cloudConnectors}
            connectionFiles={connectionFiles}
            loadingFiles={loadingFiles}
            onSelectConnection={handleSelectConnection}
            onSelectRemoteFile={handleSelectRemoteFile}
            loading={loading}
            onOpenNewConnection={onOpenNewConnection}
            testSuccess={testSuccess}
            testResultText={testResultText}
            onCopyTestResult={handleCopyTestResult}
            availableDrivers={availableDrivers}
          />
        ) : step === "select-tables" ? (
          <div className="flex-1 min-h-0 overflow-y-auto px-5 py-3">
            <TableSelectStep
              catalog={dbCatalog}
              selectedTables={selectedTables}
              onToggleTable={toggleTable}
              onImport={handleImportSelectedTables}
              onBack={() => setStep("source")}
              creating={creating}
            />
          </div>
        ) : (
          <div className="flex-1 min-h-0 overflow-y-auto px-5 py-3">
            <ConfigureStep
              form={form}
              preview={preview}
              loading={loading}
              creating={creating}
              onFormatChange={handleFormatChange}
              onBack={() => setStep("source")}
              onCreate={cloudConnectionId ? handleCreateFromCloud : handleCreate}
              isUpdateMode={isUpdateMode}
            />
          </div>
        )}

        {/* Bottom bar */}
        {step === "source" && (
          <div className="shrink-0 border-t border-border px-5 py-2.5 flex items-center justify-between">
            <div className="flex items-center gap-3">
              {(canTest || canTestDb) && (
                <button
                  type="button"
                  onClick={canTest ? (sourceType === "snowflake" ? handleTestSnowflake : handleTestPostgres) : handleTestDb}
                  disabled={testing}
                  className={`text-sm font-medium transition-colors ${
                    testSuccess ? "text-green-600 dark:text-green-400" : "text-primary hover:text-primary/80"
                  } disabled:opacity-50`}
                >
                  {testing ? "Testing..." : testSuccess ? "Test Connection \u2714" : "Test Connection"}
                </button>
              )}
              <span className="text-xs text-muted-foreground">{driverLabel}</span>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={onClose}>Cancel</Button>
              <Button
                size="sm"
                onClick={handleSourceConnect}
                disabled={
                  loading ||
                  (sourceType === "columnar" && !filePath && !(fileUrl ?? "").trim()) ||
                  (sourceType === "postgresql" && (!dbHost || !dbDatabase)) ||
                  (sourceType === "snowflake" && (!dbHost || !dbDatabase)) ||
                  ((sourceType === "sqlite" || sourceType === "duckdb") && !filePath && !(fileUrl ?? "").trim())
                }
              >
                {loading ? "Connecting..." : sourceType === "columnar" ? "Next" : "OK"}
              </Button>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

// ──── Drop zone components ────

function LocalFileDropZone({
  onPickFile,
  loading,
}: {
  onPickFile: () => void;
  loading: boolean;
}) {
  const [isDragOver, setIsDragOver] = useState(false);

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.dataTransfer.types.includes("Files")) setIsDragOver(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
  };

  return (
    <div
      className={`flex flex-col items-center gap-4 py-6 px-4 rounded-lg border-2 border-dashed transition-colors ${
        isDragOver
          ? "border-primary bg-primary/5"
          : "border-border hover:border-muted-foreground/50"
      }`}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <div className="rounded-full bg-muted p-3">
        <FileSpreadsheet className="size-6 text-muted-foreground" />
      </div>
      <p className="text-sm text-muted-foreground text-center">
        Select a local data file or drag and drop it here
      </p>
      <Button onClick={onPickFile} disabled={loading} className="gap-2">
        {loading ? (
          <Loader2 className="size-4 animate-spin" />
        ) : (
          <FolderOpen className="size-4" />
        )}
        {loading ? "Loading..." : "Choose File"}
      </Button>
      <p className="text-xs text-muted-foreground">
        CSV, TSV, JSON, JSONL, Parquet, Avro, Arrow IPC, Excel
      </p>
    </div>
  );
}

function DatabaseDropZone({
  onPickFile,
  loading,
}: {
  onPickFile: () => void;
  loading: boolean;
}) {
  const [isDragOver, setIsDragOver] = useState(false);

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.dataTransfer.types.includes("Files")) setIsDragOver(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
  };

  return (
    <div
      className={`flex flex-col items-center gap-4 py-6 px-4 rounded-lg border-2 border-dashed transition-colors ${
        isDragOver
          ? "border-primary bg-primary/5"
          : "border-border hover:border-muted-foreground/50"
      }`}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <div className="rounded-full bg-muted p-3">
        <Database className="size-6 text-muted-foreground" />
      </div>
      <p className="text-sm text-muted-foreground text-center">
        Drop a SQLite or DuckDB file here, or click to browse
      </p>
      <Button onClick={onPickFile} disabled={loading} className="gap-2">
        {loading ? (
          <Loader2 className="size-4 animate-spin" />
        ) : (
          <FolderOpen className="size-4" />
        )}
        {loading ? "Loading..." : "Choose File"}
      </Button>
      <p className="text-xs text-muted-foreground">
        SQLite (.db, .sqlite, .sqlite3) · DuckDB (.duckdb)
      </p>
    </div>
  );
}

// ──── SourceStep ────

function SourceStep({
  form,
  onSourceTypeChange,
  onPickFile,
  onPickDatabaseFile,
  cloudConnectors,
  connectionFiles,
  loadingFiles,
  onSelectConnection,
  onSelectRemoteFile,
  loading,
  onOpenNewConnection,
  testSuccess,
  testResultText,
  onCopyTestResult,
  availableDrivers,
}: {
  form: UseFormReturn<DataSourceFormValues>;
  onSourceTypeChange: (t: SourceKind) => void;
  onPickFile: () => void;
  onPickDatabaseFile: () => void;
  cloudConnectors: Connector[];
  connectionFiles: string[];
  loadingFiles: boolean;
  onSelectConnection: (id: string) => void;
  onSelectRemoteFile: (path: string) => void;
  loading: boolean;
  onOpenNewConnection?: () => void;
  testSuccess: boolean;
  testResultText: string | null;
  onCopyTestResult: () => void;
  availableDrivers: Driver[];
}) {
  const sourceType = form.watch("sourceType");
  const driver = form.watch("driver");
  const filePath = form.watch("file.path");
  const fileUrl = form.watch("file.url");
  const dbAuthMode = form.watch("db.authMode");
  const cloudConnectionId = form.watch("cloud.connectionId");

  const isPostgres = sourceType === "postgresql";
  const isSnowflake = sourceType === "snowflake";
  const isDbFile = sourceType === "sqlite" || sourceType === "duckdb";

  const driverLabel = DRIVER_OPTIONS.find((d) => d.value === driver)?.label ?? "DuckDB";

  return (
    <div className="flex-1 min-h-0 overflow-y-auto px-6 py-4">
      <div className="space-y-4">
        {/* Name row */}
        <FormRow label="Name:">
          <Input {...form.register("name")} placeholder="" />
        </FormRow>

        {/* Comment row */}
        <FormRow label="Comment:">
          <Input {...form.register("comment")} placeholder="" />
        </FormRow>

        <Separator />

        {/* Connection type + Driver row */}
        <div className="flex items-center gap-6 text-xs">
          <span className="text-muted-foreground">Connection type: <span className="text-foreground">default</span></span>
          <span className="text-muted-foreground flex items-center gap-1.5">
            Driver:
            {availableDrivers.length > 1 ? (
              <Select value={driver} onValueChange={(v) => form.setValue("driver", v as Driver)}>
                <SelectTrigger className="h-6 text-xs gap-1 px-2 w-auto min-w-[80px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {availableDrivers.map((d) => (
                    <SelectItem key={d} value={d}>
                      {DRIVER_OPTIONS.find((o) => o.value === d)?.label ?? d}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            ) : (
              <span className="text-foreground">{driverLabel}</span>
            )}
          </span>
          <div className="ml-auto">
            <Select
              value={sourceType}
              onValueChange={(v) => onSourceTypeChange(v as SourceKind)}
            >
              <SelectTrigger className="h-7 text-xs gap-1 px-2">
                <SelectValue placeholder="Select..." />
              </SelectTrigger>
              <SelectContent>
                {SOURCE_TYPE_OPTIONS.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        <Separator />

        {/* ───── SQLite / DuckDB ───── */}
        {isDbFile && (
          <>
            {!filePath && !fileUrl ? (
              <DatabaseDropZone onPickFile={onPickDatabaseFile} loading={loading} />
            ) : filePath ? (
              <>
                <FormRow label="File:">
                  <div className="flex gap-1.5 items-center">
                    <Input value={filePath} readOnly className="font-mono text-xs flex-1" />
                    <Button onClick={onPickDatabaseFile} variant="ghost" size="sm" className="h-8 px-2 shrink-0">...</Button>
                  </div>
                </FormRow>
                <FormRow label="Connection:">
                  <div>
                    <Input
                      readOnly
                      value={sourceType === "duckdb" ? `duckdb:duckdb:${filePath}` : `duckdb:sqlite:${filePath}`}
                      className="font-mono text-xs bg-muted"
                    />
                  </div>
                </FormRow>
              </>
            ) : null}

            <div className="relative flex items-center gap-2 my-1">
              <Separator className="flex-1" />
              <span className="text-[10px] text-muted-foreground px-2 uppercase tracking-wider">or load from URL</span>
              <Separator className="flex-1" />
            </div>

            <FormRow label="URL:">
              <Input
                type="url"
                {...form.register("file.url")}
                placeholder={sourceType === "duckdb" ? "https://example.com/data.duckdb" : "https://example.com/data.sqlite"}
                className="font-mono text-xs"
                disabled={loading}
              />
            </FormRow>
          </>
        )}

        {/* ───── Columnar (CSV, Parquet, Arrow, Excel) ───── */}
        {sourceType === "columnar" && (
          <>
            {!filePath ? (
              <LocalFileDropZone onPickFile={onPickFile} loading={loading} />
            ) : (
              <>
                <FormRow label="File:">
                  <div className="flex gap-1.5 items-center">
                    <Input value={filePath} readOnly className="font-mono text-xs flex-1" />
                    <Button onClick={onPickFile} variant="ghost" size="sm" className="h-8 px-2 shrink-0">...</Button>
                  </div>
                </FormRow>
              </>
            )}

            <div className="relative flex items-center gap-2 my-1">
              <Separator className="flex-1" />
              <span className="text-[10px] text-muted-foreground px-2 uppercase tracking-wider">or load from URL</span>
              <Separator className="flex-1" />
            </div>

            <FormRow label="URL:">
              <Input
                type="url"
                {...form.register("file.url")}
                placeholder="https://example.com/data.csv"
                className="font-mono text-xs"
                disabled={loading}
              />
            </FormRow>
            <FormRow label="">
              <label className="flex items-center gap-2 text-xs cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={form.watch("file.downloadLocal")}
                  onChange={(e) => form.setValue("file.downloadLocal", e.target.checked)}
                  disabled={loading}
                  className="accent-primary h-3.5 w-3.5 rounded"
                />
                <span className="text-muted-foreground">Download to local copy</span>
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Info className="h-3.5 w-3.5 text-muted-foreground/60 hover:text-muted-foreground transition-colors" />
                    </TooltipTrigger>
                    <TooltipContent side="right" className="max-w-[240px]">
                      When checked, the file is downloaded and stored locally. Otherwise it is streamed directly from the URL each time it is queried, which always reflects the latest data but requires a network connection.
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </label>
            </FormRow>
            <FormRow label="Format:">
              <Select value={form.watch("file.format")} onValueChange={(v) => form.setValue("file.format", v)} disabled={loading}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {FORMATS.map((f) => (
                    <SelectItem key={f.value} value={f.value}>{f.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </FormRow>
          </>
        )}

        {/* ───── PostgreSQL ───── */}
        {isPostgres && (
          <>
            <div className="flex gap-3">
              <FormRow label="Host:" className="flex-1">
                <Input {...form.register("db.host")} placeholder="localhost" className="font-mono" />
              </FormRow>
              <div className="w-24 shrink-0 space-y-1.5">
                <Label className="text-xs text-muted-foreground">Port:</Label>
                <Input {...form.register("db.port")} placeholder="5432" className="font-mono" />
              </div>
            </div>
            <FormRow label="Authentication:">
              <Select value={dbAuthMode} onValueChange={(v) => form.setValue("db.authMode", v as AuthMode)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="user_password">User & Password</SelectItem>
                  <SelectItem value="none">No auth</SelectItem>
                </SelectContent>
              </Select>
            </FormRow>
            {dbAuthMode === "user_password" && (
              <>
                <FormRow label="User:">
                  <Input {...form.register("db.auth.user")} placeholder="" className="font-mono" />
                </FormRow>
                <FormRow label="Password:">
                  <Input type="password" {...form.register("db.auth.password")} placeholder="" className="font-mono" />
                </FormRow>
              </>
            )}
            <FormRow label="Database:">
              <Input {...form.register("db.database")} placeholder="postgres" className="font-mono" />
            </FormRow>
            <FormRow label="URL:">
              <div>
                <Input readOnly value={`duckdb:postgresql://${form.watch("db.host")}:${form.watch("db.port") || "5432"}/${form.watch("db.database")}`} className="font-mono text-xs bg-muted" />
                <p className="text-[10px] text-muted-foreground mt-0.5">Overrides settings above</p>
              </div>
            </FormRow>
            {testSuccess && testResultText && (
              <div className="rounded-md border border-green-500/30 bg-green-500/5 p-3 space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-green-600 dark:text-green-400">Succeeded</span>
                  <Button variant="ghost" size="sm" className="h-7 gap-1" onClick={onCopyTestResult}>
                    <Copy className="size-3.5" /> Copy
                  </Button>
                </div>
                <pre className="text-xs text-muted-foreground whitespace-pre-wrap font-mono">{testResultText}</pre>
              </div>
            )}
          </>
        )}

        {/* ───── Snowflake ───── */}
        {isSnowflake && (
          <>
            <div className="flex gap-3">
              <FormRow label="Host:" className="flex-1">
                <Input {...form.register("db.host")} placeholder="" className="font-mono" />
              </FormRow>
              <div className="w-24 shrink-0 space-y-1.5">
                <Label className="text-xs text-muted-foreground">Port:</Label>
                <Input value={form.watch("db.port")} readOnly className="font-mono bg-muted" />
              </div>
            </div>
            <FormRow label="Authentication:">
              <Select value={dbAuthMode} onValueChange={(v) => form.setValue("db.authMode", v as AuthMode)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="user_password">User & Password</SelectItem>
                  <SelectItem value="none">No auth</SelectItem>
                </SelectContent>
              </Select>
            </FormRow>
            {dbAuthMode === "user_password" && (
              <>
                <FormRow label="User:">
                  <Input {...form.register("db.auth.user")} placeholder="" className="font-mono" />
                </FormRow>
                <FormRow label="Password:">
                  <Input type="password" {...form.register("db.auth.password")} placeholder="" className="font-mono" />
                </FormRow>
              </>
            )}
            <FormRow label="Database:">
              <Input {...form.register("db.database")} placeholder="" className="font-mono" />
            </FormRow>
            <FormRow label="Warehouse:">
              <Input {...form.register("db.warehouse")} placeholder="" className="font-mono" />
            </FormRow>
            <FormRow label="URL:">
              <div>
                <Input readOnly value={`duckdb:snowflake://${form.watch("db.host")}:${form.watch("db.port")}`} className="font-mono text-xs bg-muted" />
                <p className="text-[10px] text-muted-foreground mt-0.5">Overrides settings above</p>
              </div>
            </FormRow>
            {testSuccess && testResultText && (
              <div className="rounded-md border border-green-500/30 bg-green-500/5 p-3 space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-green-600 dark:text-green-400">Succeeded</span>
                  <Button variant="ghost" size="sm" className="h-7 gap-1" onClick={onCopyTestResult}>
                    <Copy className="size-3.5" /> Copy
                  </Button>
                </div>
                <pre className="text-xs text-muted-foreground whitespace-pre-wrap font-mono">{testResultText}</pre>
              </div>
            )}
          </>
        )}

        {/* ───── Cloud (S3/GCS/R2) ───── */}
        {sourceType === "connection" && (
          <>
            {cloudConnectors.length === 0 ? (
              <div className="flex flex-col items-center gap-4 py-6">
                <div className="rounded-full bg-muted p-3">
                  <Cloud className="size-6 text-muted-foreground" />
                </div>
                <p className="text-sm text-muted-foreground text-center">
                  No connections configured.
                </p>
                {onOpenNewConnection ? (
                  <Button onClick={onOpenNewConnection} variant="default" className="gap-2">
                    <Cloud className="size-4" /> Create connection
                  </Button>
                ) : (
                  <p className="text-xs text-muted-foreground text-center">Create one from the sidebar first.</p>
                )}
              </div>
            ) : (
              <>
                <FormRow label="Connection:">
                  <Select value={cloudConnectionId} onValueChange={onSelectConnection}>
                    <SelectTrigger><SelectValue placeholder="Select a connection..." /></SelectTrigger>
                    <SelectContent>
                      {cloudConnectors.map((c) => (
                        <SelectItem key={c.id} value={c.id}>
                          <span className="flex items-center gap-2">
                            <Cloud className="size-3.5" />
                            {c.name}
                            <span className="text-xs text-muted-foreground">{c.connector_type}://{c.config.bucket}</span>
                          </span>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </FormRow>
                {cloudConnectionId && (
                  <div>
                    <Label className="text-xs mb-1">Files</Label>
                    <ScrollArea className="h-48 border border-border rounded-md mt-1">
                      {loadingFiles ? (
                        <div className="flex items-center justify-center p-8">
                          <Loader2 className="size-4 animate-spin text-muted-foreground" />
                        </div>
                      ) : connectionFiles.length === 0 ? (
                        <p className="p-4 text-sm text-muted-foreground text-center">No files found</p>
                      ) : (
                        <div className="p-1">
                          {connectionFiles.map((f) => (
                            <button
                              key={f}
                              onClick={() => onSelectRemoteFile(f)}
                              disabled={loading}
                              className="w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-sm hover:bg-muted transition-colors text-left"
                            >
                              <FileSpreadsheet className="size-3.5 text-muted-foreground shrink-0" />
                              <span className="truncate font-mono text-xs">{f}</span>
                            </button>
                          ))}
                        </div>
                      )}
                    </ScrollArea>
                  </div>
                )}
              </>
            )}
          </>
        )}
      </div>
    </div>
  );
}

// ──── FormRow ────

function FormRow({
  label,
  children,
  className,
}: {
  label: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={`flex items-start gap-3 ${className ?? ""}`}>
      <Label className="text-xs text-muted-foreground w-28 shrink-0 text-right pt-2">{label}</Label>
      <div className="flex-1 min-w-0">{children}</div>
    </div>
  );
}

// ──── TableSelectStep ────

function TableSelectStep({
  catalog,
  selectedTables,
  onToggleTable,
  onImport,
  onBack,
  creating,
}: {
  catalog: CatalogEntry[];
  selectedTables: Set<string>;
  onToggleTable: (schema: string, name: string) => void;
  onImport: () => void;
  onBack: () => void;
  creating: boolean;
}) {
  const schemas = new Map<string, CatalogEntry[]>();
  for (const entry of catalog) {
    const s = entry.schema ?? "default";
    if (!schemas.has(s)) schemas.set(s, []);
    schemas.get(s)!.push(entry);
  }

  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(
    () => new Set(schemas.keys())
  );

  const toggleSchema = (schemaName: string) => {
    setExpandedSchemas((prev) => {
      const next = new Set(prev);
      if (next.has(schemaName)) next.delete(schemaName);
      else next.add(schemaName);
      return next;
    });
  };

  const toggleAllInSchema = (_schemaName: string, tables: CatalogEntry[]) => {
    const keys = tables.map(
      (e) => `${e.schema ?? "default"}.${e.name}`
    );
    const allSelected = keys.every((k) => selectedTables.has(k));
    for (const entry of tables) {
      const key = `${entry.schema ?? "default"}.${entry.name}`;
      if (allSelected && selectedTables.has(key)) {
        onToggleTable(entry.schema ?? "default", entry.name);
      } else if (!allSelected && !selectedTables.has(key)) {
        onToggleTable(entry.schema ?? "default", entry.name);
      }
    }
  };

  return (
    <div className="flex flex-col gap-3">
      <div
        className="overflow-y-auto space-y-1 rounded-md border border-border p-1"
        style={{ maxHeight: "min(50vh, 400px)" }}
      >
        {catalog.length === 0 && (
          <p className="text-sm text-muted-foreground text-center py-8">
            No tables found in this database.
          </p>
        )}
        {Array.from(schemas.entries()).map(([schemaName, tables]) => {
          const isExpanded = expandedSchemas.has(schemaName);
          const schemaKeys = tables.map(
            (e) => `${e.schema ?? "default"}.${e.name}`
          );
          const selectedCount = schemaKeys.filter((k) =>
            selectedTables.has(k)
          ).length;
          const allSelected = selectedCount === tables.length;

          return (
            <div key={schemaName} className="border border-border rounded-md overflow-hidden">
              <div
                className="flex items-center gap-2 px-3 py-2 bg-muted/50 cursor-pointer hover:bg-muted transition-colors"
                onClick={() => toggleSchema(schemaName)}
              >
                {isExpanded ? (
                  <ChevronDown className="size-3.5 text-muted-foreground shrink-0" />
                ) : (
                  <ChevronRight className="size-3.5 text-muted-foreground shrink-0" />
                )}
                <Database className="size-3.5 text-muted-foreground shrink-0" />
                <span className="text-xs font-medium">{schemaName}</span>
                <span className="text-[10px] text-muted-foreground ml-auto">
                  {tables.length} table{tables.length !== 1 ? "s" : ""}
                </span>
                {selectedCount > 0 && (
                  <Badge variant="secondary" className="text-[10px] h-4 px-1.5">
                    {selectedCount}
                  </Badge>
                )}
                <input
                  type="checkbox"
                  checked={allSelected && tables.length > 0}
                  ref={(el) => {
                    if (el) el.indeterminate = selectedCount > 0 && !allSelected;
                  }}
                  onChange={(e) => {
                    e.stopPropagation();
                    toggleAllInSchema(schemaName, tables);
                  }}
                  onClick={(e) => e.stopPropagation()}
                  className="h-3.5 w-3.5 rounded border-input ml-1"
                />
              </div>
              {isExpanded && (
                <div className="divide-y divide-border/50">
                  {tables.map((entry) => {
                    const key = `${entry.schema ?? "default"}.${entry.name}`;
                    const isSelected = selectedTables.has(key);
                    return (
                      <label
                        key={key}
                        className={`flex items-center gap-3 px-3 py-2 cursor-pointer transition-colors ${
                          isSelected
                            ? "bg-primary/10"
                            : "hover:bg-muted/30"
                        }`}
                      >
                        <input
                          type="checkbox"
                          checked={isSelected}
                          onChange={() =>
                            onToggleTable(entry.schema ?? "default", entry.name)
                          }
                          className="h-4 w-4 rounded border-input"
                        />
                        <Table2 className="size-3.5 text-muted-foreground shrink-0" />
                        <span className="font-mono text-xs font-medium">
                          {entry.name}
                        </span>
                        <Badge variant="outline" className="text-[10px] ml-auto">
                          {entry.entry_type}
                        </Badge>
                        {entry.row_count != null && (
                          <span className="text-[10px] text-muted-foreground tabular-nums">
                            {entry.row_count.toLocaleString()} rows
                          </span>
                        )}
                        <span className="text-[10px] text-muted-foreground">
                          {entry.columns.length} cols
                        </span>
                      </label>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
      <div className="flex items-center justify-between shrink-0 border-t border-border pt-3">
        <Button variant="ghost" size="sm" onClick={onBack} className="gap-1.5">
          <ArrowLeft className="size-3.5" />
          Back
        </Button>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">
            {selectedTables.size} table{selectedTables.size !== 1 ? "s" : ""}{" "}
            selected
          </span>
          <Button
            onClick={onImport}
            disabled={creating || selectedTables.size === 0}
            className="gap-2"
          >
            {creating ? (
              <Loader2 className="size-4 animate-spin" />
            ) : (
              <CheckCircle2 className="size-4" />
            )}
            {creating ? "Importing..." : "Import Selected"}
          </Button>
        </div>
      </div>
    </div>
  );
}

// ──── ConfigureStep ────

function ConfigureStep({
  form,
  preview,
  loading,
  creating,
  onFormatChange,
  onBack,
  onCreate,
  isUpdateMode,
}: {
  form: UseFormReturn<DataSourceFormValues>;
  preview: FilePreview | null;
  loading: boolean;
  creating: boolean;
  onFormatChange: (v: string) => void;
  onBack: () => void;
  onCreate: () => void;
  isUpdateMode: boolean;
}) {
  const fp = form.watch("file.path");
  const name = form.watch("name") ?? "";
  const viewName = form.watch("viewName") ?? "";
  const formatOverride = form.watch("file.format");
  const materialize = form.watch("materialize");
  const selectedPkColumn = form.watch("selectedPkColumn");

  return (
    <div className="flex-1 flex flex-col min-h-0">
      <div className="flex-1 min-h-0 overflow-y-auto space-y-4 pr-1">
      <div className="flex items-center gap-2 text-xs text-muted-foreground font-mono bg-muted rounded-md px-3 py-2 truncate">
        <FileSpreadsheet className="size-3.5 shrink-0" />
        <span className="truncate">{fp}</span>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div>
          <Label htmlFor="ds-name" className="text-xs">
            Display Name
          </Label>
          <Input
            id="ds-name"
            {...form.register("name")}
            placeholder="e.g. Sales Data 2024"
            className="mt-1"
          />
        </div>
        <div>
          <Label htmlFor="ds-view" className="text-xs">
            View Name{" "}
            <span className="text-muted-foreground">(for SQL queries)</span>
          </Label>
          <Input
            id="ds-view"
            {...form.register("viewName")}
            placeholder="e.g. sales_2024"
            className="mt-1 font-mono"
          />
        </div>
      </div>

      {!isUpdateMode && (
        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            id="materialize"
            checked={materialize}
            onChange={(e) => form.setValue("materialize", e.target.checked)}
            className="h-4 w-4 rounded border-input"
          />
          <Label htmlFor="materialize" className="text-xs font-normal cursor-pointer">
            Materialize (faster for large files; stores data in app database)
          </Label>
        </div>
      )}

      <div className="flex items-center gap-3">
        <div className="flex-1">
          <Label className="text-xs">Format</Label>
          <Select value={formatOverride} onValueChange={onFormatChange}>
            <SelectTrigger className="mt-1">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {FORMATS.map((f) => (
                <SelectItem key={f.value} value={f.value}>
                  {f.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        {preview && (
          <div className="flex items-center gap-3 pt-5">
            <Badge variant="secondary" className="font-mono text-xs">
              {preview.schema.length} columns
            </Badge>
            <Badge variant="outline" className="font-mono text-xs">
              {preview.row_count?.toLocaleString() ?? "?"} rows
            </Badge>
          </div>
        )}
      </div>

      {preview && preview.schema.length > 0 && (
        <>
          <Separator />
          <div className="flex flex-col shrink-0" style={{ height: "min(400px, 50vh)" }}>
            <Label className="text-xs mb-1.5 shrink-0">Schema Preview</Label>
            <ScrollArea className="shrink-0 border border-border rounded-md w-full" style={{ height: "min(360px, 45vh)" }}>
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="h-7 text-xs w-10">#</TableHead>
                    <TableHead className="h-7 text-xs">Column</TableHead>
                    <TableHead className="h-7 text-xs">Type</TableHead>
                    <TableHead className="h-7 text-xs">Nullable</TableHead>
                    <TableHead className="h-7 text-xs w-12">PK</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  <TableRow className="hover:bg-muted/30">
                    <TableCell className="py-1 text-xs text-muted-foreground">—</TableCell>
                    <TableCell className="py-1 text-xs text-muted-foreground italic" colSpan={2}>
                      No primary key
                    </TableCell>
                    <TableCell className="py-1" />
                    <TableCell className="py-1">
                      <input
                        type="radio"
                        name="pk-column"
                        aria-label="No primary key"
                        checked={selectedPkColumn === null}
                        onChange={() => form.setValue("selectedPkColumn", null)}
                        className="h-3.5 w-3.5"
                      />
                    </TableCell>
                  </TableRow>
                  {preview.schema.map((col, idx) => (
                    <TableRow key={col.name} className="hover:bg-muted/30">
                      <TableCell className="py-1 text-xs text-muted-foreground font-mono tabular-nums">
                        {idx + 1}
                      </TableCell>
                      <TableCell className="py-1 text-xs font-mono font-medium">
                        {col.name}
                      </TableCell>
                      <TableCell className="py-1">
                        <Badge
                          variant="outline"
                          className="font-mono text-[10px] px-1.5 py-0 h-5"
                        >
                          {col.data_type}
                        </Badge>
                      </TableCell>
                      <TableCell className="py-1 text-xs">
                        {col.nullable ? (
                          <span className="text-yellow-500">YES</span>
                        ) : (
                          <span className="text-muted-foreground">NO</span>
                        )}
                      </TableCell>
                      <TableCell className="py-1">
                        <input
                          type="radio"
                          name="pk-column"
                          aria-label={`Primary key: ${col.name}`}
                          checked={selectedPkColumn === col.name}
                          onChange={() => form.setValue("selectedPkColumn", col.name)}
                          className="h-3.5 w-3.5"
                        />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </ScrollArea>
          </div>
        </>
      )}

      {loading && (
        <div className="flex items-center justify-center py-4 text-sm text-muted-foreground gap-2">
          <Loader2 className="size-4 animate-spin" />
          Loading schema...
        </div>
      )}
      </div>

      <div className="flex items-center justify-between pt-2 shrink-0 border-t border-border mt-2 pt-3">
        <Button variant="ghost" size="sm" onClick={onBack} className="gap-1.5">
          <ArrowLeft className="size-3.5" />
          Back
        </Button>
        <Button
          onClick={onCreate}
          disabled={creating || !name.trim() || !viewName.trim() || loading}
          className="gap-2"
        >
          {creating ? (
            <Loader2 className="size-4 animate-spin" />
          ) : (
            <CheckCircle2 className="size-4" />
          )}
          {creating ? "Creating..." : "Create Data Source"}
        </Button>
      </div>
    </div>
  );
}
