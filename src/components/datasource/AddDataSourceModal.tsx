import { useState, useEffect } from "react";
import { open } from "@tauri-apps/plugin-dialog";
import {
  Loader2,
  FolderOpen,
  FileSpreadsheet,
  Cloud,
  Database,
  ArrowLeft,
  CheckCircle2,
  Server,
  ChevronDown,
  ChevronRight,
  Table2,
  Link,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
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
import type {
  DataSource,
  Connector,
  ConnectorType,
  ConnectorConfig,
  CatalogEntry,
  FilePreview,
} from "@/lib/types";
import type { ParsedError } from "@/lib/errors";
import { extractError } from "@/lib/errors";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";
import {
  previewFile,
  createDataSource,
  updateDataSource,
  listConnectionFiles,
  introspectConnector,
} from "@/lib/ipc";

interface AddDataSourceModalProps {
  open: boolean;
  onClose: () => void;
  onCreated: (ds: DataSource) => void;
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
  /** When set, opens the modal on the URL tab with this URL pre-filled */
  initialUrl?: string;
  onOpenNewConnection?: () => void;
  existingDataSource?: DataSource | null;
  onUpdated?: (ds: DataSource) => void;
}

type Step = "source" | "configure" | "select-tables";
type SourceTab = "file" | "url" | "connection" | "database" | "postgresql";

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

function fileNameToViewName(filePath: string): string {
  const name = filePath.split(/[/\\]/).pop() ?? "untitled";
  const withoutExt = name.replace(/\.[^.]+$/, "");
  return withoutExt
    .replace(/[^a-zA-Z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, "")
    .toLowerCase();
}

function fileBaseName(filePath: string): string {
  return filePath.split(/[/\\]/).pop() ?? "Untitled";
}

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
  const [step, setStep] = useState<Step>("source");
  const [sourceTab, setSourceTab] = useState<SourceTab>("file");

  const [filePath, setFilePath] = useState("");
  const [urlPath, setUrlPath] = useState("");
  const [urlFormat, setUrlFormat] = useState("csv");
  const [selectedConnectionId, setSelectedConnectionId] = useState<string>("");
  const [connectionFiles, setConnectionFiles] = useState<string[]>([]);
  const [loadingFiles, setLoadingFiles] = useState(false);

  const [name, setName] = useState("");
  const [viewName, setViewName] = useState("");
  const [formatOverride, setFormatOverride] = useState<string>("");
  const [preview, setPreview] = useState<FilePreview | null>(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ParsedError | null>(null);
  const [creating, setCreating] = useState(false);
  const [materialize, setMaterialize] = useState(false);
  const [selectedPkColumn, setSelectedPkColumn] = useState<string | null>(null);

  // File connector ID created during the current modal session
  const [fileConnectorId, setFileConnectorId] = useState<string | null>(null);

  // Database connector fields (SQLite/DuckDB combined)
  const [dbFilePath, setDbFilePath] = useState("");
  const [dbName, setDbName] = useState("");
  const [pgHost, setPgHost] = useState("localhost");
  const [pgPort, setPgPort] = useState("5432");
  const [pgDatabase, setPgDatabase] = useState("");
  const [pgUser, setPgUser] = useState("");
  const [pgPassword, setPgPassword] = useState("");
  const [dbConnectorId, setDbConnectorId] = useState<string | null>(null);
  const [dbCatalog, setDbCatalog] = useState<CatalogEntry[]>([]);
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());
  const [testing, setTesting] = useState(false);
  const [testSuccess, setTestSuccess] = useState(false);

  const cloudConnectors = connectors.filter((c) =>
    ["s3", "gcs", "r2"].includes(c.connector_type)
  );

  useEffect(() => {
    if (isOpen && existingDataSource) {
      const conn = connectors.find((c) => c.id === existingDataSource.connector_id);
      if (conn?.connector_type === "local_file") {
        setFilePath(conn.config.path ?? "");
        setName(existingDataSource.name);
        setViewName(existingDataSource.view_name ?? "");
        setFormatOverride(conn.config.format ?? "");
        setSourceTab("file");
        loadPreview(conn.config.path ?? "", conn.config.format ?? undefined);
      }
    }
  }, [isOpen, existingDataSource?.id]);

  useEffect(() => {
    if (isOpen && initialFilePath && !existingDataSource) {
      const ext = (initialFilePath.match(/\.([^.]+)$/)?.[1] ?? "").toLowerCase();
      const isDbFile = ["duckdb", "db", "sqlite", "sqlite3"].includes(ext);
      if (isDbFile) {
        setDbFilePath(initialFilePath);
        setDbName(fileBaseName(initialFilePath).replace(/\.[^.]+$/, ""));
        setSourceTab("database");
      } else {
        setFilePath(initialFilePath);
        setSourceTab("file");
        loadPreview(initialFilePath);
      }
    }
  }, [isOpen, initialFilePath, existingDataSource]);

  useEffect(() => {
    if (isOpen && initialUrl && !existingDataSource) {
      setUrlPath(initialUrl);
      setSourceTab("url");
      const pathname = initialUrl.split("?")[0];
      const ext = (pathname.match(/\.([^.]+)$/)?.[1] ?? "").toLowerCase();
      const map: Record<string, string> = {
        csv: "csv",
        tsv: "tsv",
        json: "json",
        jsonl: "jsonl",
        parquet: "parquet",
        xlsx: "xlsx",
        avro: "avro",
      };
      setUrlFormat(map[ext] ?? "csv");
    }
  }, [isOpen, initialUrl, existingDataSource]);

  useEffect(() => {
    if (!isOpen) {
      setStep("source");
      setSourceTab("file");
      setFilePath("");
      setSelectedConnectionId("");
      setConnectionFiles([]);
      setName("");
      setViewName("");
      setFormatOverride("");
      setPreview(null);
      setError(null);
      setCreating(false);
      setLoading(false);
      setMaterialize(false);
      setSelectedPkColumn(null);
      setFileConnectorId(null);
      setDbFilePath("");
      setDbName("");
      setPgHost("localhost");
      setPgPort("5432");
      setPgDatabase("");
      setPgUser("");
      setPgPassword("");
      setDbConnectorId(null);
      setDbCatalog([]);
      setSelectedTables(new Set());
      setTesting(false);
      setTestSuccess(false);
      setUrlPath("");
      setUrlFormat("csv");
    }
  }, [isOpen]);

  useEffect(() => {
    setTestSuccess(false);
  }, [pgHost, pgPort, pgDatabase, pgUser, pgPassword]);

  const loadPreview = async (path: string, format?: string) => {
    setLoading(true);
    setError(null);
    try {
      const result = await previewFile(path, format);
      setPreview(result);
      const idCol = result.schema.find(
        (c: { name: string }) => c.name.toLowerCase() === "id"
      );
      setSelectedPkColumn(idCol ? idCol.name : null);
      const baseName = fileBaseName(path);
      setName(baseName);
      setViewName(fileNameToViewName(path));
      setFormatOverride(result.format);
      setStep("configure");
    } catch (e) {
      setError(extractError(e));
    } finally {
      setLoading(false);
    }
  };

  const handlePickFile = async () => {
    const result = await open({
      multiple: false,
      filters: [
        {
          name: "Data Files",
          extensions: [
            "csv",
            "tsv",
            "json",
            "jsonl",
            "parquet",
            "xlsx",
            "avro",
            "arrow",
            "ipc",
          ],
        },
      ],
    });
    if (result) {
      setFilePath(result);
      await loadPreview(result);
    }
  };

  const handleLoadFromUrl = async () => {
    const u = urlPath.trim();
    if (!u) return;
    setFilePath(u);
    setFormatOverride(urlFormat);
    await loadPreview(u, urlFormat);
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
      setDbFilePath(result);
      setDbName(fileBaseName(result).replace(/\.[^.]+$/, ""));
    }
  };

  const handleSelectConnection = async (connId: string) => {
    setSelectedConnectionId(connId);
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
    setFilePath(path);
    await loadPreview(path);
  };

  const handleFormatChange = async (fmt: string) => {
    setFormatOverride(fmt);
    if (filePath) {
      await loadPreview(filePath, fmt);
    }
  };

  const handleCreate = async () => {
    if (!filePath || !name || !viewName) return;
    setCreating(true);
    setError(null);
    try {
      if (isUpdateMode && existingDataSource && onUpdated) {
        const ds = await updateDataSource(existingDataSource.id, {
          name,
          viewName,
        });
        onUpdated(ds);
      } else {
        let connId = fileConnectorId;
        if (!connId && onAddConnector) {
          const conn = await onAddConnector({
            name,
            connectorType: "local_file",
            config: {
              path: filePath,
              format: formatOverride || undefined,
            },
          });
          connId = conn.id;
          setFileConnectorId(connId);
        }
        if (!connId) return;

        const ds = await createDataSource({
          name,
          viewName,
          connectorId: connId,
          materialize,
          primaryKeyColumn: selectedPkColumn,
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
    if (!filePath || !name || !viewName || !selectedConnectionId) return;
    setCreating(true);
    setError(null);
    try {
      const ds = await createDataSource({
        name,
        viewName,
        connectorId: selectedConnectionId,
      });
      onCreated(ds);
      onClose();
    } catch (e) {
      setError(extractError(e));
    } finally {
      setCreating(false);
    }
  };

  const handleConnectDatabase = async () => {
    if (!dbFilePath || !dbName) return;
    const isDuckdb = dbFilePath.toLowerCase().endsWith(".duckdb");
    setLoading(true);
    setError(null);
    try {
      if (onAddConnector) {
        const conn = await onAddConnector({
          name: dbName,
          connectorType: isDuckdb ? "duckdb" : "sqlite",
          config: { path: dbFilePath },
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

  const handleTestPostgres = async () => {
    setTesting(true);
    setError(null);
    setTestSuccess(false);
    try {
      if (onTestConnector) {
        await onTestConnector({
          connectorType: "postgresql",
          config: {
            host: pgHost,
            port: parseInt(pgPort) || 5432,
            database: pgDatabase,
            user: pgUser || undefined,
          },
          secretKey: pgPassword || undefined,
        });
        setTestSuccess(true);
        setTimeout(() => setTestSuccess(false), 4000);
      }
    } catch (e) {
      setError(extractError(e));
    } finally {
      setTesting(false);
    }
  };

  const handleConnectPostgres = async () => {
    if (!dbName || !pgHost || !pgDatabase) return;
    setLoading(true);
    setError(null);
    try {
      if (onAddConnector) {
        const conn = await onAddConnector({
          name: dbName,
          connectorType: "postgresql",
          config: {
            host: pgHost,
            port: parseInt(pgPort) || 5432,
            database: pgDatabase,
            user: pgUser || undefined,
          },
          secretKey: pgPassword || undefined,
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

  const handleImportSelectedTables = async () => {
    if (!dbConnectorId || selectedTables.size === 0) return;
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

  const toggleTable = (schema: string, name: string) => {
    const key = `${schema}.${name}`;
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="w-[95vw] max-w-6xl sm:max-w-6xl min-h-[60vh] max-h-[90vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Database className="size-4" />
            {isUpdateMode ? "Import Data (Update Data Source)" : "Add Data Source"}
          </DialogTitle>
          <DialogDescription>
            {step === "source"
              ? "Choose a source type to load data from."
              : step === "select-tables"
              ? "Select the tables you want to import."
              : "Configure your data source name and review the schema."}
          </DialogDescription>
        </DialogHeader>

        {error && <ErrorDisplay error={error} compact />}

        {step === "source" ? (
          <SourceStep
            sourceTab={sourceTab}
            onTabChange={setSourceTab}
            onPickFile={handlePickFile}
            cloudConnectors={cloudConnectors}
            selectedConnectionId={selectedConnectionId}
            onSelectConnection={handleSelectConnection}
            connectionFiles={connectionFiles}
            loadingFiles={loadingFiles}
            onSelectRemoteFile={handleSelectRemoteFile}
            loading={loading}
            onOpenNewConnection={onOpenNewConnection}
            urlPath={urlPath}
            urlFormat={urlFormat}
            onUrlPathChange={setUrlPath}
            onUrlFormatChange={setUrlFormat}
            onLoadFromUrl={handleLoadFromUrl}
            dbFilePath={dbFilePath}
            dbName={dbName}
            onDbNameChange={setDbName}
            onPickDatabaseFile={handlePickDatabaseFile}
            onConnectDatabase={handleConnectDatabase}
            pgHost={pgHost}
            pgPort={pgPort}
            pgDatabase={pgDatabase}
            pgUser={pgUser}
            pgPassword={pgPassword}
            pgDbName={dbName}
            onPgHostChange={setPgHost}
            onPgPortChange={setPgPort}
            onPgDatabaseChange={setPgDatabase}
            onPgUserChange={setPgUser}
            onPgPasswordChange={setPgPassword}
            onPgDbNameChange={setDbName}
            onTestPostgres={handleTestPostgres}
            onConnectPostgres={handleConnectPostgres}
            testing={testing}
            testSuccess={testSuccess}
          />
        ) : step === "select-tables" ? (
          <TableSelectStep
            catalog={dbCatalog}
            selectedTables={selectedTables}
            onToggleTable={toggleTable}
            onImport={handleImportSelectedTables}
            onBack={() => setStep("source")}
            creating={creating}
          />
        ) : (
          <ConfigureStep
            filePath={filePath}
            name={name}
            onNameChange={setName}
            viewName={viewName}
            onViewNameChange={setViewName}
            formatOverride={formatOverride}
            onFormatChange={handleFormatChange}
            preview={preview}
            loading={loading}
            creating={creating}
            materialize={materialize}
            onMaterializeChange={setMaterialize}
            selectedPkColumn={selectedPkColumn}
            onSelectedPkColumn={setSelectedPkColumn}
            onBack={() => setStep("source")}
            onCreate={selectedConnectionId ? handleCreateFromCloud : handleCreate}
            isUpdateMode={isUpdateMode}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

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
    // Path is set by Tauri window onDragDropEvent in AppLayout; modal receives it via initialFilePath and loads preview
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
    // Path is set by Tauri window onDragDropEvent in AppLayout; modal receives it via initialFilePath
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

function SourceStep({
  sourceTab,
  onTabChange,
  onPickFile,
  urlPath,
  urlFormat,
  onUrlPathChange,
  onUrlFormatChange,
  onLoadFromUrl,
  cloudConnectors,
  selectedConnectionId,
  onSelectConnection,
  connectionFiles,
  loadingFiles,
  onSelectRemoteFile,
  loading,
  onOpenNewConnection,
  dbFilePath,
  dbName,
  onDbNameChange,
  onPickDatabaseFile,
  onConnectDatabase,
  pgHost,
  pgPort,
  pgDatabase,
  pgUser,
  pgPassword,
  pgDbName,
  onPgHostChange,
  onPgPortChange,
  onPgDatabaseChange,
  onPgUserChange,
  onPgPasswordChange,
  onPgDbNameChange,
  onTestPostgres,
  onConnectPostgres,
  testing,
  testSuccess,
}: {
  sourceTab: SourceTab;
  onTabChange: (tab: SourceTab) => void;
  onPickFile: () => void;
  urlPath: string;
  urlFormat: string;
  onUrlPathChange: (v: string) => void;
  onUrlFormatChange: (v: string) => void;
  onLoadFromUrl: () => void;
  cloudConnectors: Connector[];
  selectedConnectionId: string;
  onSelectConnection: (id: string) => void;
  connectionFiles: string[];
  loadingFiles: boolean;
  onSelectRemoteFile: (path: string) => void;
  loading: boolean;
  onOpenNewConnection?: () => void;
  dbFilePath: string;
  dbName: string;
  onDbNameChange: (v: string) => void;
  onPickDatabaseFile: () => void;
  onConnectDatabase: () => void;
  pgHost: string;
  pgPort: string;
  pgDatabase: string;
  pgUser: string;
  pgPassword: string;
  pgDbName: string;
  onPgHostChange: (v: string) => void;
  onPgPortChange: (v: string) => void;
  onPgDatabaseChange: (v: string) => void;
  onPgUserChange: (v: string) => void;
  onPgPasswordChange: (v: string) => void;
  onPgDbNameChange: (v: string) => void;
  onTestPostgres: () => void;
  onConnectPostgres: () => void;
  testing: boolean;
  testSuccess: boolean;
}) {
  return (
    <div className="space-y-4">
      <div className="flex gap-1 p-0.5 bg-muted rounded-md">
        <button
          onClick={() => onTabChange("file")}
          className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded text-sm font-medium transition-colors ${
            sourceTab === "file"
              ? "bg-background text-foreground shadow-sm"
              : "text-muted-foreground hover:text-foreground"
          }`}
        >
          <FolderOpen className="size-4" />
          Local File
        </button>
        <button
          onClick={() => onTabChange("url")}
          className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded text-sm font-medium transition-colors ${
            sourceTab === "url"
              ? "bg-background text-foreground shadow-sm"
              : "text-muted-foreground hover:text-foreground"
          }`}
        >
          <Link className="size-4" />
          URL
        </button>
        <button
          onClick={() => onTabChange("database")}
          className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded text-sm font-medium transition-colors ${
            sourceTab === "database"
              ? "bg-background text-foreground shadow-sm"
              : "text-muted-foreground hover:text-foreground"
          }`}
        >
          <Database className="size-4" />
          SQLite/DuckDB
        </button>
        <button
          onClick={() => onTabChange("postgresql")}
          className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded text-sm font-medium transition-colors ${
            sourceTab === "postgresql"
              ? "bg-background text-foreground shadow-sm"
              : "text-muted-foreground hover:text-foreground"
          }`}
        >
          <Server className="size-4" />
          PostgreSQL
        </button>
        <button
          onClick={() => onTabChange("connection")}
          className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded text-sm font-medium transition-colors ${
            sourceTab === "connection"
              ? "bg-background text-foreground shadow-sm"
              : "text-muted-foreground hover:text-foreground"
          }`}
        >
          <Cloud className="size-4" />
          Cloud Storage
        </button>
      </div>

      {sourceTab === "file" && (
        <LocalFileDropZone
          onPickFile={onPickFile}
          loading={loading}
        />
      )}

      {sourceTab === "url" && (
        <div className="space-y-4 py-4">
          <div className="flex flex-col items-center gap-3">
            <div className="rounded-full bg-muted p-3">
              <Link className="size-6 text-muted-foreground" />
            </div>
            <p className="text-sm text-muted-foreground text-center">
              Load a file from an HTTP(S) URL. DuckDB fetches it via the httpfs extension.
            </p>
          </div>
          <div className="space-y-2">
            <Label className="text-xs">URL</Label>
            <Input
              type="url"
              value={urlPath}
              onChange={(e) => onUrlPathChange(e.target.value)}
              placeholder="https://example.com/data.csv"
              className="font-mono text-xs"
              disabled={loading}
            />
          </div>
          <div className="space-y-2">
            <Label className="text-xs">Format</Label>
            <Select value={urlFormat} onValueChange={onUrlFormatChange} disabled={loading}>
              <SelectTrigger>
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
          <Button
            onClick={onLoadFromUrl}
            disabled={!urlPath.trim() || loading}
            className="w-full gap-2"
          >
            {loading ? (
              <Loader2 className="size-4 animate-spin" />
            ) : (
              <Link className="size-4" />
            )}
            {loading ? "Loading…" : "Load from URL"}
          </Button>
        </div>
      )}

      {sourceTab === "database" && (
        <div className="space-y-4 py-4">
          {!dbFilePath ? (
            <DatabaseDropZone onPickFile={onPickDatabaseFile} loading={loading} />
          ) : (
            <>
              <div className="flex gap-2 items-end">
                <div className="flex-1">
                  <Label className="text-xs">Database File</Label>
                  <Input
                    value={dbFilePath}
                    readOnly
                    placeholder="Select a .db / .sqlite / .duckdb file..."
                    className="mt-1 font-mono text-xs"
                  />
                </div>
                <Button onClick={onPickDatabaseFile} variant="outline" className="gap-1.5">
                  <FolderOpen className="size-4" />
                  Browse
                </Button>
              </div>
              <div>
                <Label className="text-xs">Connection Name</Label>
                <Input
                  value={dbName}
                  onChange={(e) => onDbNameChange(e.target.value)}
                  placeholder="e.g. my_database"
                  className="mt-1"
                />
              </div>
              <Button
                onClick={onConnectDatabase}
                disabled={!dbFilePath || !dbName || loading}
                className="w-full gap-2"
              >
                {loading ? <Loader2 className="size-4 animate-spin" /> : <Database className="size-4" />}
                {loading ? "Connecting..." : "Connect & Browse Tables"}
              </Button>
            </>
          )}
        </div>
      )}

      {sourceTab === "postgresql" && (
        <div className="space-y-3 py-2">
          <div>
            <Label className="text-xs">Connection Name</Label>
            <Input
              value={pgDbName}
              onChange={(e) => onPgDbNameChange(e.target.value)}
              placeholder="e.g. production_db"
              className="mt-1"
            />
          </div>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <Label className="text-xs">Host</Label>
              <Input
                value={pgHost}
                onChange={(e) => onPgHostChange(e.target.value)}
                placeholder="localhost"
                className="mt-1 font-mono"
              />
            </div>
            <div>
              <Label className="text-xs">Port</Label>
              <Input
                value={pgPort}
                onChange={(e) => onPgPortChange(e.target.value)}
                placeholder="5432"
                className="mt-1 font-mono"
              />
            </div>
          </div>
          <div>
            <Label className="text-xs">Database</Label>
            <Input
              value={pgDatabase}
              onChange={(e) => onPgDatabaseChange(e.target.value)}
              placeholder="mydb"
              className="mt-1 font-mono"
            />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <Label className="text-xs">User</Label>
              <Input
                value={pgUser}
                onChange={(e) => onPgUserChange(e.target.value)}
                placeholder="postgres"
                className="mt-1 font-mono"
              />
            </div>
            <div>
              <Label className="text-xs">Password</Label>
              <Input
                type="password"
                value={pgPassword}
                onChange={(e) => onPgPasswordChange(e.target.value)}
                placeholder="••••••"
                className="mt-1 font-mono"
              />
            </div>
          </div>
          <div className="flex gap-2 pt-1">
            <Button
              variant={testSuccess ? "outline" : "outline"}
              onClick={onTestPostgres}
              disabled={testing || !pgHost || !pgDatabase}
              className={`gap-1.5 transition-colors ${
                testSuccess
                  ? "border-green-500 bg-green-500/10 text-green-600 hover:bg-green-500/15 hover:text-green-600"
                  : ""
              }`}
            >
              {testing ? (
                <Loader2 className="size-4 animate-spin" />
              ) : testSuccess ? (
                <CheckCircle2 className="size-4 text-green-500" />
              ) : (
                <CheckCircle2 className="size-4" />
              )}
              {testSuccess ? "Connected" : "Test Connection"}
            </Button>
            <Button
              onClick={onConnectPostgres}
              disabled={loading || !pgDbName || !pgHost || !pgDatabase}
              className="flex-1 gap-1.5"
            >
              {loading ? <Loader2 className="size-4 animate-spin" /> : <Server className="size-4" />}
              {loading ? "Connecting..." : "Connect & Browse Tables"}
            </Button>
          </div>
        </div>
      )}

      {sourceTab === "connection" && (
        <div className="space-y-3">
          {cloudConnectors.length === 0 ? (
            <div className="flex flex-col items-center gap-4 py-6">
              <div className="rounded-full bg-muted p-3">
                <Cloud className="size-6 text-muted-foreground" />
              </div>
              <p className="text-sm text-muted-foreground text-center">
                No connections configured. Create a connection to browse files from S3, GCP, or Cloudflare R2.
              </p>
              {onOpenNewConnection ? (
                <Button onClick={onOpenNewConnection} variant="default" className="gap-2">
                  <Cloud className="size-4" />
                  Create connection
                </Button>
              ) : (
                <p className="text-xs text-muted-foreground text-center">
                  Create one from the sidebar first.
                </p>
              )}
            </div>
          ) : (
            <>
              <div>
                <Label className="text-xs">Connection</Label>
                <Select
                  value={selectedConnectionId}
                  onValueChange={onSelectConnection}
                >
                  <SelectTrigger className="mt-1">
                    <SelectValue placeholder="Select a connection..." />
                  </SelectTrigger>
                  <SelectContent>
                    {cloudConnectors.map((c) => (
                      <SelectItem key={c.id} value={c.id}>
                        <span className="flex items-center gap-2">
                          <Cloud className="size-3.5" />
                          {c.name}
                          <span className="text-xs text-muted-foreground">
                            {c.connector_type}://{c.config.bucket}
                          </span>
                        </span>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {selectedConnectionId && (
                <div>
                  <Label className="text-xs mb-1">Files</Label>
                  <ScrollArea className="h-48 border border-border rounded-md mt-1">
                    {loadingFiles ? (
                      <div className="flex items-center justify-center p-8">
                        <Loader2 className="size-4 animate-spin text-muted-foreground" />
                      </div>
                    ) : connectionFiles.length === 0 ? (
                      <p className="p-4 text-sm text-muted-foreground text-center">
                        No files found
                      </p>
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
                            <span className="truncate font-mono text-xs">
                              {f}
                            </span>
                          </button>
                        ))}
                      </div>
                    )}
                  </ScrollArea>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

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

function ConfigureStep({
  filePath,
  name,
  onNameChange,
  viewName,
  onViewNameChange,
  formatOverride,
  onFormatChange,
  preview,
  loading,
  creating,
  materialize,
  onMaterializeChange,
  selectedPkColumn,
  onSelectedPkColumn,
  onBack,
  onCreate,
  isUpdateMode,
}: {
  filePath: string;
  name: string;
  onNameChange: (v: string) => void;
  viewName: string;
  onViewNameChange: (v: string) => void;
  formatOverride: string;
  onFormatChange: (v: string) => void;
  preview: FilePreview | null;
  loading: boolean;
  creating: boolean;
  materialize: boolean;
  onMaterializeChange: (v: boolean) => void;
  selectedPkColumn: string | null;
  onSelectedPkColumn: (col: string | null) => void;
  onBack: () => void;
  onCreate: () => void;
  isUpdateMode: boolean;
}) {
  return (
    <div className="flex-1 flex flex-col min-h-0">
      <div className="flex-1 min-h-0 overflow-y-auto space-y-4 pr-1">
      <div className="flex items-center gap-2 text-xs text-muted-foreground font-mono bg-muted rounded-md px-3 py-2 truncate">
        <FileSpreadsheet className="size-3.5 shrink-0" />
        <span className="truncate">{filePath}</span>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div>
          <Label htmlFor="ds-name" className="text-xs">
            Display Name
          </Label>
          <Input
            id="ds-name"
            value={name}
            onChange={(e) => onNameChange(e.target.value)}
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
            value={viewName}
            onChange={(e) => onViewNameChange(e.target.value)}
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
            onChange={(e) => onMaterializeChange(e.target.checked)}
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
                        onChange={() => onSelectedPkColumn(null)}
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
                          onChange={() => onSelectedPkColumn(col.name)}
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
