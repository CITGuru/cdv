import { useState, useEffect, useMemo } from "react";
import {
  Loader2,
  ArrowLeft,
  ArrowRight,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Database,
  AlertCircle,
  Plus,
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
import type {
  Connector,
  ConnectorConfig,
  ConnectorType,
  CatalogEntry,
  SyncStrategy,
} from "@/lib/types";
import { SYNC_STRATEGIES } from "@/lib/types";
import type { ParsedError } from "@/lib/errors";
import { extractError } from "@/lib/errors";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";
import { introspectConnector } from "@/lib/ipc";

type WizardStep = "source" | "destination" | "storage" | "strategy" | "review";
const STEPS: WizardStep[] = ["source", "destination", "storage", "strategy", "review"];
const STEP_LABELS: Record<WizardStep, string> = {
  source: "Source",
  destination: "Destination",
  storage: "Storage",
  strategy: "Strategy & Tables",
  review: "Review",
};

interface EtlJobModalProps {
  open: boolean;
  onClose: () => void;
  connectors: Connector[];
  onAddConnector?: (params: {
    name: string;
    connectorType: ConnectorType;
    config: ConnectorConfig;
  }) => Promise<Connector>;
  onSubmit: (params: {
    name: string;
    sourceConnectorId: string;
    targetConnectorId: string;
    strategy: SyncStrategy;
    includeSchemas?: string[];
    excludeTables?: string[];
    skipViews?: boolean;
    batchSize?: number;
    runNow?: boolean;
  }) => Promise<void>;
}

type DuckLakeCatalogType = "duckdb" | "postgres" | "sqlite";

interface NewDuckLakeForm {
  name: string;
  catalogType: DuckLakeCatalogType;
  metadataPath: string;
  dataPath: string;
  pgHost: string;
  pgPort: string;
  pgDatabase: string;
  pgUser: string;
  pgPassword: string;
}

const EMPTY_DL_FORM: NewDuckLakeForm = {
  name: "",
  catalogType: "duckdb",
  metadataPath: "",
  dataPath: "",
  pgHost: "localhost",
  pgPort: "5432",
  pgDatabase: "",
  pgUser: "",
  pgPassword: "",
};

export function EtlJobModal({ open, onClose, connectors, onAddConnector, onSubmit }: EtlJobModalProps) {
  const [step, setStep] = useState<WizardStep>("source");
  const [jobName, setJobName] = useState("");
  const [sourceId, setSourceId] = useState("");
  const [targetId, setTargetId] = useState("");
  const [strategy, setStrategy] = useState<SyncStrategy>("full");
  const [skipViews, setSkipViews] = useState(true);
  const [selectedSchemas, setSelectedSchemas] = useState<Set<string>>(new Set());
  const [selectAllSchemas, setSelectAllSchemas] = useState(true);
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [replicationKeys, setReplicationKeys] = useState<Record<string, string>>({});

  const [sourceCatalog, setSourceCatalog] = useState<CatalogEntry[]>([]);
  const [sourceLoading, setSourceLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<ParsedError | null>(null);

  const [destMode, setDestMode] = useState<"existing" | "create">("existing");
  const [newDlForm, setNewDlForm] = useState<NewDuckLakeForm>({ ...EMPTY_DL_FORM });
  const [creatingDl, setCreatingDl] = useState(false);
  const [createdConnector, setCreatedConnector] = useState<Connector | null>(null);

  const pgConnectors = useMemo(
    () => connectors.filter((c) => c.connector_type === "postgresql"),
    [connectors]
  );
  const dlConnectors = useMemo(
    () => connectors.filter((c) => c.connector_type === "ducklake"),
    [connectors]
  );

  useEffect(() => {
    if (!open) {
      setStep("source");
      setJobName("");
      setSourceId("");
      setTargetId("");
      setStrategy("full");
      setSkipViews(true);
      setSelectedSchemas(new Set());
      setSelectAllSchemas(true);
      setExpandedSchemas(new Set());
      setReplicationKeys({});
      setSourceCatalog([]);
      setError(null);
      setDestMode("existing");
      setNewDlForm({ ...EMPTY_DL_FORM });
      setCreatingDl(false);
      setCreatedConnector(null);
    }
  }, [open]);

  useEffect(() => {
    if (sourceId && open) {
      setSourceLoading(true);
      introspectConnector(sourceId)
        .then((entries) => {
          setSourceCatalog(entries);
          const schemas = new Set(
            entries.map((e) => e.schema ?? "public")
          );
          setSelectedSchemas(schemas);
          setSelectAllSchemas(true);
        })
        .catch(() => setSourceCatalog([]))
        .finally(() => setSourceLoading(false));
    }
  }, [sourceId, open]);

  // Derive storage info from target DuckLake connector (or the just-created one)
  const targetConnector =
    createdConnector && createdConnector.id === targetId
      ? createdConnector
      : connectors.find((c) => c.id === targetId);
  const dataPath = targetConnector?.config.data_path ?? "";
  const storageOk = dataPath.startsWith("s3://");

  const schemas = useMemo(() => {
    const map = new Map<string, CatalogEntry[]>();
    for (const entry of sourceCatalog) {
      const s = entry.schema ?? "public";
      if (!map.has(s)) map.set(s, []);
      map.get(s)!.push(entry);
    }
    return map;
  }, [sourceCatalog]);

  const filteredTables = useMemo(() => {
    return sourceCatalog.filter((e) => {
      if (skipViews && e.entry_type === "view") return false;
      const s = e.schema ?? "public";
      if (!selectAllSchemas && !selectedSchemas.has(s)) return false;
      return true;
    });
  }, [sourceCatalog, skipViews, selectAllSchemas, selectedSchemas]);

  const totalEstimatedRows = useMemo(() => {
    return filteredTables.reduce((sum, t) => sum + (t.row_count ?? 0), 0);
  }, [filteredTables]);

  const sourceConnector = connectors.find((c) => c.id === sourceId);

  useEffect(() => {
    if (sourceConnector && targetConnector && !jobName) {
      setJobName(
        `${sourceConnector.name} → ${targetConnector.name}`
      );
    }
  }, [sourceConnector, targetConnector, jobName]);

  const stepIndex = STEPS.indexOf(step);

  const newDlFormValid = (() => {
    if (!newDlForm.dataPath.trim()) return false;
    if (newDlForm.catalogType === "postgres") {
      return !!newDlForm.pgDatabase.trim();
    }
    return !!newDlForm.metadataPath.trim();
  })();

  const canNext = (() => {
    switch (step) {
      case "source": return !!sourceId;
      case "destination":
        if (destMode === "existing") return !!targetId;
        return newDlFormValid && !creatingDl;
      case "storage": return storageOk;
      case "strategy": return filteredTables.length > 0;
      case "review": return jobName.trim().length > 0;
    }
  })();

  const buildDuckLakeConfig = (): ConnectorConfig => {
    const cfg: ConnectorConfig = {
      catalog_type: newDlForm.catalogType,
      data_path: newDlForm.dataPath || undefined,
    };
    if (newDlForm.catalogType === "postgres") {
      const parts = [
        `dbname=${newDlForm.pgDatabase}`,
        `host=${newDlForm.pgHost}`,
        `port=${newDlForm.pgPort || "5432"}`,
      ];
      if (newDlForm.pgUser) parts.push(`user=${newDlForm.pgUser}`);
      if (newDlForm.pgPassword) parts.push(`password=${newDlForm.pgPassword}`);
      cfg.metadata_path = `postgres:${parts.join(" ")}`;
    } else if (newDlForm.catalogType === "sqlite") {
      cfg.metadata_path = `sqlite:${newDlForm.metadataPath}`;
    } else {
      cfg.metadata_path = newDlForm.metadataPath;
    }
    return cfg;
  };

  const goNext = async () => {
    const idx = STEPS.indexOf(step);
    if (idx >= STEPS.length - 1) return;

    if (step === "destination" && destMode === "create" && !targetId) {
      if (!onAddConnector) return;
      setCreatingDl(true);
      setError(null);
      try {
        const name = newDlForm.name.trim() || (
          newDlForm.catalogType === "postgres"
            ? newDlForm.pgDatabase
            : newDlForm.metadataPath.split(/[/\\]/).pop()?.replace(/\.[^.]+$/, "") ?? "ducklake"
        );
        const conn = await onAddConnector({
          name,
          connectorType: "ducklake",
          config: buildDuckLakeConfig(),
        });
        setCreatedConnector(conn);
        setTargetId(conn.id);
        setStep(STEPS[idx + 1]);
      } catch (e) {
        setError(extractError(e));
      } finally {
        setCreatingDl(false);
      }
      return;
    }

    setStep(STEPS[idx + 1]);
  };

  const goBack = () => {
    const idx = STEPS.indexOf(step);
    if (idx > 0) setStep(STEPS[idx - 1]);
  };

  const handleSubmit = async (runNow: boolean) => {
    setSubmitting(true);
    setError(null);
    try {
      const includeSchemas = selectAllSchemas
        ? undefined
        : [...selectedSchemas];

      await onSubmit({
        name: jobName.trim(),
        sourceConnectorId: sourceId,
        targetConnectorId: targetId,
        strategy,
        includeSchemas,
        skipViews,
        runNow,
      });
      onClose();
    } catch (e) {
      setError(extractError(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="w-[95vw] max-w-2xl sm:max-w-2xl min-h-[500px] max-h-[90vh] flex flex-col gap-0 p-0">
        <div className="flex items-center gap-3 px-6 pt-5 pb-3">
          <Database className="size-5 text-primary" />
          <DialogTitle className="text-base font-semibold">
            Create ETL Job
          </DialogTitle>
        </div>

        {/* Step indicator */}
        <div className="flex items-center gap-1 px-6 pb-3">
          {STEPS.map((s, i) => (
            <div key={s} className="flex items-center gap-1">
              <button
                onClick={() => i <= stepIndex && setStep(s)}
                disabled={i > stepIndex}
                className={`text-xs px-2 py-1 rounded-md transition-colors ${
                  s === step
                    ? "bg-primary text-primary-foreground"
                    : i < stepIndex
                      ? "bg-muted text-foreground cursor-pointer hover:bg-muted/80"
                      : "text-muted-foreground"
                }`}
              >
                {STEP_LABELS[s]}
              </button>
              {i < STEPS.length - 1 && (
                <ChevronRight className="size-3 text-muted-foreground" />
              )}
            </div>
          ))}
        </div>

        <Separator />

        {/* Body */}
        <ScrollArea className="flex-1 min-h-0 px-6 py-4">
          {step === "source" && (
            <div className="space-y-4">
              <div>
                <h3 className="text-sm font-medium mb-1">Select PostgreSQL Source</h3>
                <p className="text-xs text-muted-foreground mb-3">
                  Choose the PostgreSQL database to extract data from.
                </p>
              </div>
              {pgConnectors.length === 0 ? (
                <div className="flex items-center gap-2 p-4 rounded-lg border border-dashed border-border">
                  <AlertCircle className="size-4 text-muted-foreground" />
                  <span className="text-sm text-muted-foreground">
                    No PostgreSQL connectors found. Add one first via Data Source &rarr; PostgreSQL.
                  </span>
                </div>
              ) : (
                <Select value={sourceId} onValueChange={setSourceId}>
                  <SelectTrigger>
                    <SelectValue placeholder="Select a PostgreSQL connection" />
                  </SelectTrigger>
                  <SelectContent>
                    {pgConnectors.map((c) => (
                      <SelectItem key={c.id} value={c.id}>
                        {c.name}
                        {c.config.host && (
                          <span className="text-muted-foreground ml-2 text-xs">
                            ({c.config.host}:{c.config.port ?? 5432}/{c.config.database ?? ""})
                          </span>
                        )}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
              {sourceLoading && (
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="size-4 animate-spin" />
                  Introspecting database...
                </div>
              )}
              {!sourceLoading && sourceId && sourceCatalog.length > 0 && (
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <CheckCircle2 className="size-4 text-green-500" />
                  Found {sourceCatalog.length} tables/views across {schemas.size} schemas
                </div>
              )}
            </div>
          )}

          {step === "destination" && (
            <div className="space-y-4">
              <div>
                <h3 className="text-sm font-medium mb-1">DuckLake Destination</h3>
                <p className="text-xs text-muted-foreground mb-3">
                  Select an existing DuckLake catalog or create a new one.
                </p>
              </div>

              {/* Toggle: existing vs create */}
              <div className="flex gap-2">
                <button
                  onClick={() => { setDestMode("existing"); setError(null); }}
                  className={`flex-1 text-xs px-3 py-2 rounded-lg border transition-colors ${
                    destMode === "existing"
                      ? "border-primary bg-primary/5 font-medium"
                      : "border-border hover:border-muted-foreground/30"
                  }`}
                >
                  Use Existing
                </button>
                {onAddConnector && (
                  <button
                    onClick={() => { setDestMode("create"); setTargetId(""); setError(null); }}
                    className={`flex-1 text-xs px-3 py-2 rounded-lg border transition-colors flex items-center justify-center gap-1.5 ${
                      destMode === "create"
                        ? "border-primary bg-primary/5 font-medium"
                        : "border-border hover:border-muted-foreground/30"
                    }`}
                  >
                    <Plus className="size-3" />
                    Create New
                  </button>
                )}
              </div>

              {destMode === "existing" && (
                <>
                  {dlConnectors.length === 0 ? (
                    <div className="flex items-center gap-2 p-4 rounded-lg border border-dashed border-border">
                      <AlertCircle className="size-4 text-muted-foreground" />
                      <span className="text-sm text-muted-foreground">
                        No DuckLake connectors found.
                        {onAddConnector && " Switch to \"Create New\" to set one up."}
                      </span>
                    </div>
                  ) : (
                    <Select value={targetId} onValueChange={setTargetId}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select a DuckLake connection" />
                      </SelectTrigger>
                      <SelectContent>
                        {dlConnectors.map((c) => (
                          <SelectItem key={c.id} value={c.id}>
                            {c.name}
                            {c.config.metadata_path && (
                              <span className="text-muted-foreground ml-2 text-xs">
                                ({c.config.catalog_type ?? "duckdb"})
                              </span>
                            )}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  )}
                </>
              )}

              {destMode === "create" && (
                <div className="space-y-3 rounded-lg border border-border p-4">
                  <div className="space-y-1.5">
                    <Label className="text-xs">Connection Name</Label>
                    <Input
                      value={newDlForm.name}
                      onChange={(e) => setNewDlForm((f) => ({ ...f, name: e.target.value }))}
                      placeholder="e.g. ducklake-analytics"
                      className="text-sm"
                    />
                  </div>

                  <div className="space-y-1.5">
                    <Label className="text-xs">Catalog Type</Label>
                    <Select
                      value={newDlForm.catalogType}
                      onValueChange={(v) =>
                        setNewDlForm((f) => ({ ...f, catalogType: v as DuckLakeCatalogType, metadataPath: "" }))
                      }
                    >
                      <SelectTrigger className="text-sm">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="duckdb">DuckDB File</SelectItem>
                        <SelectItem value="postgres">PostgreSQL</SelectItem>
                        <SelectItem value="sqlite">SQLite</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  {newDlForm.catalogType === "duckdb" && (
                    <div className="space-y-1.5">
                      <Label className="text-xs">Metadata File</Label>
                      <Input
                        value={newDlForm.metadataPath}
                        onChange={(e) => setNewDlForm((f) => ({ ...f, metadataPath: e.target.value }))}
                        placeholder="metadata.ducklake"
                        className="font-mono text-xs"
                      />
                    </div>
                  )}

                  {newDlForm.catalogType === "sqlite" && (
                    <div className="space-y-1.5">
                      <Label className="text-xs">SQLite File</Label>
                      <Input
                        value={newDlForm.metadataPath}
                        onChange={(e) => setNewDlForm((f) => ({ ...f, metadataPath: e.target.value }))}
                        placeholder="metadata.sqlite"
                        className="font-mono text-xs"
                      />
                    </div>
                  )}

                  {newDlForm.catalogType === "postgres" && (
                    <>
                      <div className="flex gap-3">
                        <div className="flex-1 space-y-1.5">
                          <Label className="text-xs">Host</Label>
                          <Input
                            value={newDlForm.pgHost}
                            onChange={(e) => setNewDlForm((f) => ({ ...f, pgHost: e.target.value }))}
                            placeholder="localhost"
                            className="font-mono text-xs"
                          />
                        </div>
                        <div className="w-24 shrink-0 space-y-1.5">
                          <Label className="text-xs">Port</Label>
                          <Input
                            value={newDlForm.pgPort}
                            onChange={(e) => setNewDlForm((f) => ({ ...f, pgPort: e.target.value }))}
                            placeholder="5432"
                            className="font-mono text-xs"
                          />
                        </div>
                      </div>
                      <div className="space-y-1.5">
                        <Label className="text-xs">Database</Label>
                        <Input
                          value={newDlForm.pgDatabase}
                          onChange={(e) => setNewDlForm((f) => ({ ...f, pgDatabase: e.target.value }))}
                          placeholder="ducklake_catalog"
                          className="font-mono text-xs"
                        />
                      </div>
                      <div className="flex gap-3">
                        <div className="flex-1 space-y-1.5">
                          <Label className="text-xs">User</Label>
                          <Input
                            value={newDlForm.pgUser}
                            onChange={(e) => setNewDlForm((f) => ({ ...f, pgUser: e.target.value }))}
                            className="font-mono text-xs"
                          />
                        </div>
                        <div className="flex-1 space-y-1.5">
                          <Label className="text-xs">Password</Label>
                          <Input
                            type="password"
                            value={newDlForm.pgPassword}
                            onChange={(e) => setNewDlForm((f) => ({ ...f, pgPassword: e.target.value }))}
                            className="font-mono text-xs"
                          />
                        </div>
                      </div>
                    </>
                  )}

                  <Separator />

                  <div className="space-y-1.5">
                    <Label className="text-xs">Data Path (R2 / S3 Storage)</Label>
                    <Input
                      value={newDlForm.dataPath}
                      onChange={(e) => setNewDlForm((f) => ({ ...f, dataPath: e.target.value }))}
                      placeholder="s3://my-bucket/ducklake-data/"
                      className="font-mono text-xs"
                    />
                    <p className="text-[10px] text-muted-foreground">
                      S3-compatible URL where DuckLake stores Parquet files (e.g. s3://bucket/path/)
                    </p>
                  </div>

                  {newDlForm.dataPath && newDlForm.dataPath.startsWith("s3://") && (
                    <div className="flex items-center gap-2 p-2 rounded-md bg-green-500/10 border border-green-500/20">
                      <CheckCircle2 className="size-3.5 text-green-500" />
                      <span className="text-xs text-green-700 dark:text-green-400">
                        R2 storage path configured
                      </span>
                    </div>
                  )}

                  {creatingDl && (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="size-4 animate-spin" />
                      Creating DuckLake connection...
                    </div>
                  )}
                </div>
              )}

              {error && <ErrorDisplay error={error} />}
            </div>
          )}

          {step === "storage" && (
            <div className="space-y-4">
              <div>
                <h3 className="text-sm font-medium mb-1">Storage Configuration</h3>
                <p className="text-xs text-muted-foreground mb-3">
                  DuckLake writes Parquet files to the configured data path.
                  For R2, this should be an s3:// URI.
                </p>
              </div>
              {targetConnector && (
                <div className="space-y-3">
                  <div className="grid grid-cols-[120px_1fr] gap-2 text-sm">
                    <span className="text-muted-foreground">Metadata</span>
                    <span className="font-mono text-xs truncate">
                      {targetConnector.config.metadata_path ?? "—"}
                    </span>
                    <span className="text-muted-foreground">Data path</span>
                    <span className="font-mono text-xs truncate">
                      {dataPath || "—"}
                    </span>
                    <span className="text-muted-foreground">Catalog type</span>
                    <span>{targetConnector.config.catalog_type ?? "duckdb"}</span>
                  </div>
                  {storageOk ? (
                    <div className="flex items-center gap-2 p-3 rounded-lg bg-green-500/10 border border-green-500/20">
                      <CheckCircle2 className="size-4 text-green-500" />
                      <span className="text-sm text-green-700 dark:text-green-400">
                        Storage is configured on R2 ({dataPath})
                      </span>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2 p-3 rounded-lg bg-destructive/10 border border-destructive/20">
                      <AlertCircle className="size-4 text-destructive" />
                      <span className="text-sm text-destructive">
                        Data path must start with s3:// for R2 storage.
                        Update the DuckLake connector&apos;s data_path first.
                      </span>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {step === "strategy" && (
            <div className="space-y-4">
              <div>
                <h3 className="text-sm font-medium mb-1">Sync Strategy</h3>
                <p className="text-xs text-muted-foreground mb-3">
                  Choose how data should be synchronized.
                </p>
              </div>

              <div className="grid gap-2">
                {SYNC_STRATEGIES.map((s) => (
                  <button
                    key={s.value}
                    onClick={() => setStrategy(s.value)}
                    className={`flex items-start gap-3 p-3 rounded-lg border text-left transition-colors ${
                      strategy === s.value
                        ? "border-primary bg-primary/5"
                        : "border-border hover:border-muted-foreground/30"
                    }`}
                  >
                    <div
                      className={`mt-0.5 size-4 rounded-full border-2 flex items-center justify-center shrink-0 ${
                        strategy === s.value ? "border-primary" : "border-muted-foreground/40"
                      }`}
                    >
                      {strategy === s.value && (
                        <div className="size-2 rounded-full bg-primary" />
                      )}
                    </div>
                    <div>
                      <div className="text-sm font-medium">{s.label}</div>
                      <div className="text-xs text-muted-foreground">{s.description}</div>
                    </div>
                  </button>
                ))}
              </div>

              <Separator />

              <div>
                <h3 className="text-sm font-medium mb-2">Table Selection</h3>
                <div className="flex items-center gap-4 mb-3">
                  <label className="flex items-center gap-2 text-sm">
                    <input
                      type="checkbox"
                      checked={skipViews}
                      onChange={(e) => setSkipViews(e.target.checked)}
                      className="rounded"
                    />
                    Skip views
                  </label>
                  <label className="flex items-center gap-2 text-sm">
                    <input
                      type="checkbox"
                      checked={selectAllSchemas}
                      onChange={(e) => {
                        setSelectAllSchemas(e.target.checked);
                        if (e.target.checked) {
                          setSelectedSchemas(new Set(schemas.keys()));
                        }
                      }}
                      className="rounded"
                    />
                    All schemas
                  </label>
                </div>

                <div className="border border-border rounded-lg max-h-48 overflow-auto">
                  {[...schemas.entries()].map(([schema, tables]) => {
                    const isExpanded = expandedSchemas.has(schema);
                    const isSelected = selectAllSchemas || selectedSchemas.has(schema);
                    const visibleTables = tables.filter(
                      (t) => !(skipViews && t.entry_type === "view")
                    );
                    return (
                      <div key={schema}>
                        <div className="flex items-center gap-2 px-3 py-1.5 hover:bg-muted/50">
                          {!selectAllSchemas && (
                            <input
                              type="checkbox"
                              checked={isSelected}
                              onChange={(e) => {
                                const next = new Set(selectedSchemas);
                                if (e.target.checked) next.add(schema);
                                else next.delete(schema);
                                setSelectedSchemas(next);
                              }}
                              className="rounded"
                            />
                          )}
                          <button
                            onClick={() => {
                              const next = new Set(expandedSchemas);
                              if (isExpanded) next.delete(schema);
                              else next.add(schema);
                              setExpandedSchemas(next);
                            }}
                            className="flex items-center gap-1 text-sm font-medium flex-1 text-left"
                          >
                            {isExpanded ? (
                              <ChevronDown className="size-3" />
                            ) : (
                              <ChevronRight className="size-3" />
                            )}
                            {schema}
                            <Badge variant="secondary" className="ml-1 text-[10px]">
                              {visibleTables.length}
                            </Badge>
                          </button>
                        </div>
                        {isExpanded && (
                          <div className="pl-8 pb-1">
                            {visibleTables.map((t) => (
                              <div
                                key={`${schema}.${t.name}`}
                                className="flex items-center justify-between px-2 py-1 text-xs"
                              >
                                <span className="font-mono truncate">{t.name}</span>
                                <span className="text-muted-foreground shrink-0 ml-2">
                                  {t.entry_type === "view" ? "view" : ""}
                                  {t.row_count != null ? ` ~${t.row_count.toLocaleString()} rows` : ""}
                                </span>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>

                <p className="text-xs text-muted-foreground mt-2">
                  {filteredTables.length} tables selected
                  {totalEstimatedRows > 0 &&
                    ` · ~${totalEstimatedRows.toLocaleString()} estimated rows`}
                </p>
              </div>

              {strategy === "incremental" && (
                <>
                  <Separator />
                  <div>
                    <h3 className="text-sm font-medium mb-1">Replication Keys</h3>
                    <p className="text-xs text-muted-foreground mb-3">
                      For incremental sync, specify a column (timestamp or monotonic ID)
                      to track new/updated rows. Leave empty for tables without a clear key.
                    </p>
                    <div className="space-y-2 max-h-40 overflow-auto">
                      {filteredTables.slice(0, 20).map((t) => {
                        const key = `${t.schema ?? "public"}.${t.name}`;
                        const candidates = t.columns.filter(
                          (c) =>
                            c.data_type.toLowerCase().includes("timestamp") ||
                            c.data_type.toLowerCase().includes("date") ||
                            c.data_type.toLowerCase().includes("int") ||
                            c.data_type.toLowerCase().includes("serial")
                        );
                        return (
                          <div key={key} className="flex items-center gap-2">
                            <span className="text-xs font-mono truncate w-40 shrink-0">
                              {t.name}
                            </span>
                            <Select
                              value={replicationKeys[key] ?? ""}
                              onValueChange={(v) =>
                                setReplicationKeys((prev) => ({ ...prev, [key]: v }))
                              }
                            >
                              <SelectTrigger className="h-7 text-xs">
                                <SelectValue placeholder="Select column..." />
                              </SelectTrigger>
                              <SelectContent>
                                {candidates.map((col) => (
                                  <SelectItem key={col.name} value={col.name}>
                                    {col.name} ({col.data_type})
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>
                        );
                      })}
                      {filteredTables.length > 20 && (
                        <p className="text-xs text-muted-foreground italic">
                          ...and {filteredTables.length - 20} more tables
                        </p>
                      )}
                    </div>
                  </div>
                </>
              )}
            </div>
          )}

          {step === "review" && (
            <div className="space-y-4">
              <div>
                <h3 className="text-sm font-medium mb-1">Review ETL Job</h3>
                <p className="text-xs text-muted-foreground mb-3">
                  Confirm the configuration before creating the job.
                </p>
              </div>

              <div>
                <Label className="text-xs">Job Name</Label>
                <Input
                  value={jobName}
                  onChange={(e) => setJobName(e.target.value)}
                  placeholder="e.g. pg-prod → ducklake-analytics"
                  className="mt-1"
                />
              </div>

              <div className="rounded-lg border border-border p-4 space-y-2 text-sm">
                <div className="grid grid-cols-[100px_1fr] gap-1">
                  <span className="text-muted-foreground">Source</span>
                  <span className="font-medium">{sourceConnector?.name ?? "—"}</span>
                  <span className="text-muted-foreground">Destination</span>
                  <span className="font-medium">{targetConnector?.name ?? "—"}</span>
                  <span className="text-muted-foreground">Storage</span>
                  <span className="font-mono text-xs">{dataPath}</span>
                  <span className="text-muted-foreground">Strategy</span>
                  <span>{SYNC_STRATEGIES.find((s) => s.value === strategy)?.label}</span>
                  <span className="text-muted-foreground">Tables</span>
                  <span>{filteredTables.length} tables</span>
                  <span className="text-muted-foreground">Est. rows</span>
                  <span>~{totalEstimatedRows.toLocaleString()}</span>
                </div>
              </div>

              {error && <ErrorDisplay error={error} />}
            </div>
          )}
        </ScrollArea>

        <Separator />

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-3">
          <div>
            {stepIndex > 0 && (
              <Button variant="ghost" size="sm" onClick={goBack} disabled={submitting}>
                <ArrowLeft className="size-4 mr-1" />
                Back
              </Button>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Button variant="ghost" size="sm" onClick={onClose} disabled={submitting}>
              Cancel
            </Button>
            {step !== "review" ? (
              <Button size="sm" onClick={goNext} disabled={!canNext}>
                Next
                <ArrowRight className="size-4 ml-1" />
              </Button>
            ) : (
              <>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handleSubmit(false)}
                  disabled={!canNext || submitting}
                >
                  {submitting ? <Loader2 className="size-4 animate-spin mr-1" /> : null}
                  Save Job
                </Button>
                <Button
                  size="sm"
                  onClick={() => handleSubmit(true)}
                  disabled={!canNext || submitting}
                >
                  {submitting ? <Loader2 className="size-4 animate-spin mr-1" /> : null}
                  Save &amp; Run Now
                </Button>
              </>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
