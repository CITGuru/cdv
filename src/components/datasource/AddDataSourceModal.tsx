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
import type { DataSource, ConnectionInfo, FilePreview } from "@/lib/types";
import type { ParsedError } from "@/lib/errors";
import { extractError } from "@/lib/errors";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";
import {
  previewFile,
  createDataSource,
  updateDataSource,
  listConnectionFiles,
} from "@/lib/ipc";

interface AddDataSourceModalProps {
  open: boolean;
  onClose: () => void;
  onCreated: (ds: DataSource) => void;
  connections: ConnectionInfo[];
  initialFilePath?: string;
  onOpenNewConnection?: () => void;
  /** When set, modal is in "Import" (update) mode: prefill from this source and call onUpdated on submit */
  existingDataSource?: DataSource | null;
  onUpdated?: (ds: DataSource) => void;
}

type Step = "source" | "configure";
type SourceTab = "file" | "connection";

const FORMATS = [
  { value: "csv", label: "CSV" },
  { value: "tsv", label: "TSV" },
  { value: "json", label: "JSON" },
  { value: "jsonl", label: "JSONL" },
  { value: "parquet", label: "Parquet" },
  { value: "xlsx", label: "Excel" },
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
  connections,
  initialFilePath,
  onOpenNewConnection,
  existingDataSource,
  onUpdated,
}: AddDataSourceModalProps) {
  const isUpdateMode = !!existingDataSource;
  const [step, setStep] = useState<Step>("source");
  const [sourceTab, setSourceTab] = useState<SourceTab>("file");

  const [filePath, setFilePath] = useState("");
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

  useEffect(() => {
    if (isOpen && existingDataSource) {
      setFilePath(existingDataSource.path);
      setName(existingDataSource.name);
      setViewName(existingDataSource.view_name);
      setFormatOverride(existingDataSource.format || "");
      setSelectedConnectionId(existingDataSource.connection_id || "");
      setSourceTab(existingDataSource.connection_id ? "connection" : "file");
      loadPreview(existingDataSource.path, existingDataSource.format);
    }
  }, [isOpen, existingDataSource?.id]);

  useEffect(() => {
    if (isOpen && initialFilePath && !existingDataSource) {
      setFilePath(initialFilePath);
      setSourceTab("file");
      loadPreview(initialFilePath);
    }
  }, [isOpen, initialFilePath, existingDataSource]);

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
    }
  }, [isOpen]);

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
          path: filePath,
          name,
          viewName,
          format: formatOverride || undefined,
          connectionId: selectedConnectionId || undefined,
        });
        onUpdated(ds);
      } else {
        const ds = await createDataSource({
          name,
          viewName,
          path: filePath,
          format: formatOverride || undefined,
          connectionId: selectedConnectionId || undefined,
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
              ? "Choose a local file or remote connection to load data from."
              : "Configure your data source name and review the schema."}
          </DialogDescription>
        </DialogHeader>

        {error && <ErrorDisplay error={error} compact />}

        {step === "source" ? (
          <SourceStep
            sourceTab={sourceTab}
            onTabChange={setSourceTab}
            onPickFile={handlePickFile}
            connections={connections}
            selectedConnectionId={selectedConnectionId}
            onSelectConnection={handleSelectConnection}
            connectionFiles={connectionFiles}
            loadingFiles={loadingFiles}
            onSelectRemoteFile={handleSelectRemoteFile}
            loading={loading}
            onOpenNewConnection={onOpenNewConnection}
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
            onCreate={handleCreate}
            isUpdateMode={isUpdateMode}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function SourceStep({
  sourceTab,
  onTabChange,
  onPickFile,
  connections,
  selectedConnectionId,
  onSelectConnection,
  connectionFiles,
  loadingFiles,
  onSelectRemoteFile,
  loading,
  onOpenNewConnection,
}: {
  sourceTab: SourceTab;
  onTabChange: (tab: SourceTab) => void;
  onPickFile: () => void;
  connections: ConnectionInfo[];
  selectedConnectionId: string;
  onSelectConnection: (id: string) => void;
  connectionFiles: string[];
  loadingFiles: boolean;
  onSelectRemoteFile: (path: string) => void;
  loading: boolean;
  onOpenNewConnection?: () => void;
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
          onClick={() => onTabChange("connection")}
          className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded text-sm font-medium transition-colors ${
            sourceTab === "connection"
              ? "bg-background text-foreground shadow-sm"
              : "text-muted-foreground hover:text-foreground"
          }`}
        >
          <Cloud className="size-4" />
          From Connection
        </button>
      </div>

      {sourceTab === "file" ? (
        <div className="flex flex-col items-center gap-4 py-6">
          <div className="rounded-full bg-muted p-3">
            <FileSpreadsheet className="size-6 text-muted-foreground" />
          </div>
          <p className="text-sm text-muted-foreground text-center">
            Select a local data file to add as a source
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
            CSV, TSV, JSON, JSONL, Parquet, Arrow IPC
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {connections.length === 0 ? (
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
                    {connections.map((c) => (
                      <SelectItem key={c.id} value={c.id}>
                        <span className="flex items-center gap-2">
                          <Cloud className="size-3.5" />
                          {c.name}
                          <span className="text-xs text-muted-foreground">
                            {c.provider === "gcp" ? "gcs" : c.provider === "cloudflare" ? "r2" : "s3"}://{c.bucket}
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
