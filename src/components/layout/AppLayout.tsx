import { useState, useCallback, useEffect, useRef } from "react";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { Table2, TerminalSquare, Download, FolderOpen, Plus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Sidebar } from "./Sidebar";
import { DatasetViewer } from "@/components/dataset/DatasetViewer";
import { QueryEditor } from "@/components/query/QueryEditor";
import { ResultsTable } from "@/components/query/ResultsTable";
import { ConnectionManager } from "@/components/cloud/ConnectionManager";
import { ExportModal } from "@/components/export/ExportModal";
import { AddDataSourceModal } from "@/components/datasource/AddDataSourceModal";
import { SettingsModal } from "@/components/settings/SettingsModal";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useDataset } from "@/hooks/useDataset";
import { useSettings } from "@/hooks/useSettings";
import { useQueryEngine } from "@/hooks/useQuery";
import { useWorkspaceTabs } from "@/hooks/useWorkspaceTabs";
import { useConnectors } from "@/hooks/useConnectors";
import type { DataTab, QueryTab, ViewMode, WorkspaceTab } from "@/hooks/useWorkspaceTabs";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import {
  listDataSources,
  getPersistedTabs,
  setPersistedTabs,
  removeDataSource as removeDataSourceIpc,
  createDataSource as createDataSourceIpc,
  addConnector as addConnectorIpc,
  checkGraphSupport,
  installGraphExtension,
  listPropertyGraphs,
  dropPropertyGraph as dropPropertyGraphIpc,
} from "@/lib/ipc";
import type {
  DataSource,
  Connector,
  PropertyGraphInfo,
  AddConnectorResult,
} from "@/lib/types";
import { CreateGraphModal } from "@/components/graph/CreateGraphModal";
import { AlgorithmPanel } from "@/components/graph/AlgorithmPanel";
import { useEtlJobs } from "@/hooks/useEtlJobs";
import { EtlJobModal } from "@/components/etl/EtlJobModal";
import { EtlProgress } from "@/components/etl/EtlProgress";

const SIDEBAR_MIN = 180;
const SIDEBAR_MAX = 480;

export function AppLayout() {
  const dataset = useDataset();
  const settings = useSettings();
  const queryEngine = useQueryEngine();
  const tabs = useWorkspaceTabs();
  const connectorsHook = useConnectors();
  const [showAddSource, setShowAddSource] = useState(false);
  const [showAddFromUrl, setShowAddFromUrl] = useState(false);
  const [showConnections, setShowConnections] = useState(false);
  const [showExport, setShowExport] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [exportQuery, setExportQuery] = useState("");
  const [querySql, setQuerySql] = useState("");
  const [dropFilePath, setDropFilePath] = useState<string | undefined>();
  const [showProperties, setShowProperties] = useState<DataSource | null>(null);
  const [showImportFor, setShowImportFor] = useState<DataSource | null>(null);
  const [editingQueryTabId, setEditingQueryTabId] = useState<string | null>(null);
  const [editingTabValue, setEditingTabValue] = useState("");
  const lastActiveDataTabRef = useRef<{ tabId: string; dataSourceId: string; viewMode?: ViewMode } | null>(null);
  const hasHydratedTabsRef = useRef(false);
  const [graphSupported, setGraphSupported] = useState(false);
  const [propertyGraphs, setPropertyGraphs] = useState<PropertyGraphInfo[]>([]);
  const [showCreateGraph, setShowCreateGraph] = useState(false);
  const [showAlgorithm, setShowAlgorithm] = useState<string | null>(null);

  const etlHook = useEtlJobs();
  const [showEtlModal, setShowEtlModal] = useState(false);
  const [showEtlProgress, setShowEtlProgress] = useState(false);
  const [activeEtlJobId, setActiveEtlJobId] = useState<string | null>(null);

  const sidebarWidth = Math.max(
    SIDEBAR_MIN,
    Math.min(SIDEBAR_MAX, settings.settings.sidebar_width)
  );
  const isResizingRef = useRef(false);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isResizingRef.current = true;
    const startX = e.clientX;
    const startWidth = sidebarWidth;
    const onMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientX - startX;
      const next = Math.max(SIDEBAR_MIN, Math.min(SIDEBAR_MAX, startWidth + delta));
      settings.updateSettings({ sidebar_width: next });
    };
    const onUp = () => {
      isResizingRef.current = false;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, [sidebarWidth, settings]);

  useEffect(() => {
    const currentWindow = getCurrentWindow();
    const unlisten = currentWindow.onDragDropEvent(async (event) => {
      if (event.payload.type === "drop") {
        const path = event.payload.paths[0];
        if (path) {
          setDropFilePath(path);
          setShowAddSource(true);
        }
      }
    });
    return () => {
      unlisten.then((fn) => fn());
    };
  }, []);

  const refreshGraphs = useCallback(async () => {
    try {
      const supported = await checkGraphSupport();
      setGraphSupported(supported);
      if (supported) {
        const graphs = await listPropertyGraphs();
        setPropertyGraphs(graphs);
      }
    } catch {
      setGraphSupported(false);
    }
  }, []);

  const handleCreateGraph = useCallback(async () => {
    if (graphSupported) {
      setShowCreateGraph(true);
      return;
    }
    try {
      await installGraphExtension();
      setGraphSupported(true);
      setShowCreateGraph(true);
    } catch (e) {
      console.error("Failed to install DuckPGQ:", e);
      alert(
        "Could not install the DuckPGQ graph extension.\n\n" +
        "This requires an active internet connection to download the extension from the DuckDB community repository.\n\n" +
        "Please check your connection and try again."
      );
    }
  }, [graphSupported]);

  useEffect(() => {
    connectorsHook.loadConnectors();
    refreshGraphs();
    etlHook.loadJobs();
    listDataSources()
      .then((sources) => {
        dataset.setDataSources(sources);
        return getPersistedTabs().then((workspace) => ({ workspace, sources }));
      })
      .then(({ workspace, sources }) => {
        const sourceIds = new Set(sources.map((ds) => ds.id));
        const filtered = workspace.openTabs.filter(
          (t) => t.type === "query" || sourceIds.has(t.dataSourceId)
        );
        if (!hasHydratedTabsRef.current) {
          if (filtered.length > 0 || workspace.activeTabId != null) {
            tabs.hydrateTabs(filtered as WorkspaceTab[], workspace.activeTabId);
          }
          hasHydratedTabsRef.current = true;
        }
      })
      .catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!hasHydratedTabsRef.current) return;
    setPersistedTabs({
      openTabs: tabs.openTabs,
      activeTabId: tabs.activeTabId ?? null,
    }).catch(() => {});
  }, [tabs.openTabs, tabs.activeTabId]);

  const activeTab = tabs.activeTab;
  const activeDataTab = activeTab?.type === "data" ? (activeTab as DataTab) : null;
  const activeQueryTab = activeTab?.type === "query" ? (activeTab as QueryTab) : null;

  useEffect(() => {
    queryEngine.setActiveQueryTabId(activeQueryTab?.id ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeQueryTab?.id]);
  const activeDataSource = activeDataTab
    ? dataset.dataSources.find((ds) => ds.id === activeDataTab.dataSourceId) ?? null
    : null;

  useEffect(() => {
    if (!activeDataTab || !activeDataSource) return;
    const last = lastActiveDataTabRef.current;
    if (
      last?.tabId === activeDataTab.id &&
      last?.dataSourceId === activeDataSource.id &&
      last?.viewMode === (activeDataTab.viewMode ?? undefined)
    ) {
      return;
    }
    lastActiveDataTabRef.current = {
      tabId: activeDataTab.id,
      dataSourceId: activeDataSource.id,
      viewMode: activeDataTab.viewMode,
    };
    dataset.selectSource(activeDataSource, activeDataTab.viewMode);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeDataTab?.id, activeDataTab?.viewMode, activeDataSource?.id]);

  useEffect(() => {
    const qTab = activeQueryTab;
    if (qTab?.autoExecute && qTab.initialSql?.trim()) {
      queryEngine.executeQuery(qTab.initialSql, {
        useStreaming: settings.settings.streaming_enabled,
        tabId: qTab.id,
      });
      tabs.updateTab(qTab.id, { autoExecute: false });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeQueryTab?.id, activeQueryTab?.autoExecute, activeQueryTab?.initialSql, settings.settings.streaming_enabled]);

  const handleDataSourceSelect = useCallback(
    (ds: DataSource, viewMode?: ViewMode) => {
      tabs.openDataTab(ds, viewMode);
    },
    [tabs]
  );

  const handleNewQuery = useCallback(
    (ds: DataSource) => {
      const ref = ds.qualified_name;
      tabs.openQueryTab(`SELECT * FROM ${ref}`);
      setQuerySql(`SELECT * FROM ${ref}`);
    },
    [tabs]
  );

  function buildViewSql(ds: DataSource, viewMode: ViewMode): string {
    const ref = ds.qualified_name;
    const total = ds.row_count ?? 0;
    switch (viewMode) {
      case "first100":
        return `SELECT * FROM ${ref}`;
      case "last100": {
        const offset = Math.max(0, total - 100);
        return `SELECT * FROM ${ref} LIMIT 100 OFFSET ${offset}`;
      }
      case "all":
        return `SELECT * FROM ${ref}`;
      case "filtered":
      default:
        return `SELECT * FROM ${ref}`;
    }
  }

  const handleViewDataAsQuery = useCallback(
    (ds: DataSource, viewMode: ViewMode) => {
      const tabName = ds.name;
      const sql = buildViewSql(ds, viewMode);
      setQuerySql(sql);
      tabs.openQueryTab(sql, { autoExecute: true, name: tabName });
    },
    [tabs]
  );

  const handleQuerySelect = useCallback((sql: string) => {
    setQuerySql(sql);
    tabs.openQueryTab(sql);
  }, [tabs]);

  const handleDataSourceCreated = useCallback(
    (ds: DataSource, opts?: { openTab?: boolean }) => {
      dataset.addDataSource(ds);
      if (opts?.openTab !== false) {
        tabs.openDataTab(ds);
      }
    },
    [dataset, tabs]
  );

  const handleConnectionCreated = useCallback((conn: Connector) => {
    connectorsHook.setConnectors((prev) => {
      if (prev.find((c) => c.id === conn.id)) return prev;
      return [...prev, conn];
    });
  }, [connectorsHook]);

  const handleRemoveDataSource = useCallback(
    async (id: string) => {
      try {
        await removeDataSourceIpc(id);
        dataset.removeSource(id);
        tabs.closeTabsForDataSource(id);
      } catch {
        // silently fail
      }
    },
    [dataset, tabs]
  );

  const handleRemoveConnector = useCallback(async (id: string) => {
    try {
      await connectorsHook.removeConnector(id);
      const toRemove = dataset.dataSources.filter((ds) => ds.connector_id === id);
      for (const ds of toRemove) {
        dataset.removeSource(ds.id);
        tabs.closeTabsForDataSource(ds.id);
      }
    } catch {
      // silently fail
    }
  }, [connectorsHook, dataset, tabs]);

  const handleRefreshConnector = useCallback(async (id: string) => {
    await connectorsHook.refreshCatalog(id);
  }, [connectorsHook]);

  const handleConnectorExpand = useCallback(async (connectorId: string) => {
    await connectorsHook.loadCatalog(connectorId);
  }, [connectorsHook]);

  const handleRefreshDataSource = useCallback(
    (ds: DataSource) => {
      dataset.refreshSource(ds);
    },
    [dataset]
  );

  const handleOpenAddSource = useCallback(() => {
    setDropFilePath(undefined);
    setShowAddSource(true);
  }, []);

  const handleOpenQueryConsole = useCallback(() => {
    tabs.openQueryTab();
  }, [tabs]);

  const handleAddFolder = useCallback(async () => {
    const paths = await openDialog({
      multiple: true,
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
    const pathList = Array.isArray(paths) ? paths : paths ? [paths] : [];
    if (pathList.length === 0) return;
    const created: DataSource[] = [];
    for (const p of pathList) {
      try {
        const name = p.split(/[/\\]/).pop() ?? "untitled";
        const viewName = name.replace(/\.[^.]+$/, "").replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
        const ext = (name.match(/\.([^.]+)$/)?.[1] ?? "").toLowerCase();
        const formatMap: Record<string, string> = {
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
        const format = formatMap[ext] ?? "csv";
        const conn = await addConnectorIpc({
          name: name,
          connectorType: "local_file",
          config: { path: p, format },
        });
        const ds = await createDataSourceIpc({
          name,
          viewName,
          connectorId: conn.id,
        });
        dataset.addDataSource(ds);
        created.push(ds);
      } catch {
        // skip failed file
      }
    }
    if (created.length > 0) tabs.openDataTab(created[0]);
  }, [dataset, tabs]);

  const handleAddFromUrl = useCallback(() => {
    setShowAddFromUrl(true);
  }, []);

  const handleExport = useCallback((ds: DataSource) => {
    setExportQuery(`SELECT * FROM ${ds.qualified_name}`);
    setShowExport(true);
  }, []);

  const handleImport = useCallback((ds: DataSource) => {
    setShowImportFor(ds);
  }, []);

  const handleProperties = useCallback((ds: DataSource) => {
    setShowProperties(ds);
  }, []);

  const handleImportDbTable = useCallback(
    async (
      connectorId: string,
      database: string | undefined,
      schema: string,
      tableName: string
    ) => {
      try {
        const ds = await createDataSourceIpc({
          name: tableName,
          viewName: tableName,
          connectorId,
          dbSchema: schema,
          dbTable: tableName,
          dbDatabase: database ?? null,
        });
        dataset.addDataSource(ds);
        tabs.openDataTab(ds);
      } catch {
        // silently fail
      }
    },
    [dataset, tabs]
  );

  const handleNewQueryFromTable = useCallback(
    (qualifiedName: string) => {
      const sql = `SELECT * FROM ${qualifiedName}`;
      setQuerySql(sql);
      tabs.openQueryTab(sql);
    },
    [tabs]
  );

  const handleGraphQuery = useCallback(
    (graphName: string) => {
      const sql = `SELECT * FROM GRAPH_TABLE (${graphName}\n    MATCH (a)-[e]->(b)\n    COLUMNS (a.*, e.*, b.*)\n)`;
      setQuerySql(sql);
      tabs.openQueryTab(sql);
    },
    [tabs]
  );

  const handleDropGraph = useCallback(
    async (name: string) => {
      try {
        await dropPropertyGraphIpc(name);
        setPropertyGraphs((prev) => prev.filter((g) => g.name !== name));
      } catch {
        // silently fail
      }
    },
    []
  );

  const handleCreateEtlJob = useCallback(() => setShowEtlModal(true), []);

  const handleEtlSubmit = useCallback(
    async (params: {
      name: string;
      sourceConnectorId: string;
      targetConnectorId: string;
      strategy: import("@/lib/types").SyncStrategy;
      includeSchemas?: string[];
      excludeTables?: string[];
      skipViews?: boolean;
      batchSize?: number;
      runNow?: boolean;
    }) => {
      const job = await etlHook.createJob(params);
      if (params.runNow) {
        await etlHook.runJob(job.id);
        setActiveEtlJobId(job.id);
        setShowEtlProgress(true);
      }
    },
    [etlHook]
  );

  const handleRunEtlJob = useCallback(
    async (jobId: string) => {
      await etlHook.runJob(jobId);
      setActiveEtlJobId(jobId);
      setShowEtlProgress(true);
    },
    [etlHook]
  );

  const handleViewEtlProgress = useCallback((jobId: string) => {
    setActiveEtlJobId(jobId);
    setShowEtlProgress(true);
  }, []);

  const activeEtlJob = activeEtlJobId
    ? etlHook.jobs.find((j) => j.id === activeEtlJobId) ?? null
    : null;

  const hasTabs = tabs.openTabs.length > 0;

  return (
    <div className="flex h-screen bg-background text-foreground">
      <div
        className="flex shrink-0 flex-col h-full"
        style={{ width: sidebarWidth, minWidth: sidebarWidth }}
      >
        <Sidebar
          dataSources={dataset.dataSources}
          connectors={connectorsHook.connectors}
          catalogs={connectorsHook.catalogs}
          connectorBrowse={connectorsHook.connectorBrowse}
          catalogLoading={connectorsHook.catalogLoading}
          dbListLoading={connectorsHook.dbListLoading}
          activeSourceId={dataset.activeSource?.id ?? null}
          queryHistory={queryEngine.history}
          propertyGraphs={propertyGraphs}
          graphSupported={graphSupported}
          onAddDataSource={handleOpenAddSource}
          onOpenQueryConsole={handleOpenQueryConsole}
          onAddFolder={handleAddFolder}
          onAddFromUrl={handleAddFromUrl}
          onCreateGraph={handleCreateGraph}
          onDropGraph={handleDropGraph}
          onGraphQuery={handleGraphQuery}
          onRunAlgorithm={(name) => setShowAlgorithm(name)}
          onDataSourceSelect={handleDataSourceSelect}
          onDataSourceRemove={handleRemoveDataSource}
          onConnectorRemove={handleRemoveConnector}
          onConnectorRefresh={handleRefreshConnector}
          onConnectorExpand={handleConnectorExpand}
          onDataSourceRefresh={handleRefreshDataSource}
          onQuerySelect={handleQuerySelect}
          onNewQuery={handleNewQuery}
          onOpenDataTab={handleDataSourceSelect}
          onViewDataAsQuery={handleViewDataAsQuery}
          onExport={handleExport}
          onImport={handleImport}
          onProperties={handleProperties}
          onOpenSettings={() => setShowSettings(true)}
          onImportDbTable={handleImportDbTable}
          onConnectDatabase={connectorsHook.connectDatabase}
          onNewQueryFromTable={handleNewQueryFromTable}
          etlJobs={etlHook.jobs}
          etlActiveProgress={etlHook.activeProgress}
          onCreateEtlJob={handleCreateEtlJob}
          onRunEtlJob={handleRunEtlJob}
          onCancelEtlJob={etlHook.cancelJob}
          onDeleteEtlJob={etlHook.deleteJob}
          onViewEtlProgress={handleViewEtlProgress}
        />
      </div>
      <div
        role="separator"
        aria-label="Resize sidebar"
        onMouseDown={handleResizeStart}
        className="w-1 shrink-0 cursor-col-resize border-r border-border bg-transparent hover:bg-border/80 transition-colors group"
      >
        <div className="w-0.5 h-full group-hover:bg-primary/50 transition-colors mx-auto" />
      </div>
      <main className="flex-1 flex flex-col overflow-hidden min-w-0">
        <div className="flex items-center justify-between h-10 border-b border-border bg-card px-2 shrink-0 gap-2">
          <div className="flex items-center gap-2 min-w-0 flex-1">
            <div className="flex items-center gap-0.5 overflow-x-auto scrollbar-thin min-w-0 flex-1">
              {tabs.openTabs.map((tab) => {
                const label =
                  tab.type === "data"
                    ? (dataset.dataSources.find((ds) => ds.id === tab.dataSourceId)?.name ?? tab.dataSourceId)
                    : (tab as QueryTab).name ?? "Query";
                const isActive = tab.id === tabs.activeTabId;
                const isEditingQuery = tab.type === "query" && editingQueryTabId === tab.id;
                return (
                  <div
                    key={tab.id}
                    className={`flex items-center gap-1 shrink-0 rounded-t-md border border-b-0 px-2 py-1.5 text-xs cursor-pointer transition-colors ${
                      isActive
                        ? "bg-background border-border border-b-transparent -mb-px"
                        : "bg-muted/50 border-transparent hover:bg-muted"
                    }`}
                    onClick={() => !isEditingQuery && tabs.setActiveTab(tab.id)}
                  >
                    {tab.type === "data" ? (
                      <Table2 className="size-3.5 text-muted-foreground shrink-0" />
                    ) : (
                      <TerminalSquare className="size-3.5 text-muted-foreground shrink-0" />
                    )}
                    {isEditingQuery ? (
                      <input
                        type="text"
                        className="min-w-[80px] max-w-[140px] px-0.5 py-0 bg-transparent border-b border-foreground/30 text-inherit text-xs outline-none focus:border-primary"
                        value={editingTabValue}
                        onChange={(e) => setEditingTabValue(e.target.value)}
                        onBlur={() => {
                          const trimmed = editingTabValue.trim();
                          tabs.updateTab(tab.id, { name: trimmed || (tab as QueryTab).name || "Query" });
                          setEditingQueryTabId(null);
                        }}
                        onKeyDown={(e) => {
                          if (e.key === "Enter") {
                            e.currentTarget.blur();
                          }
                          e.stopPropagation();
                        }}
                        onClick={(e) => e.stopPropagation()}
                        autoFocus
                      />
                    ) : (
                      <span
                        className="truncate max-w-[120px]"
                        onDoubleClick={(e) => {
                          if (tab.type === "query") {
                            e.stopPropagation();
                            setEditingQueryTabId(tab.id);
                            setEditingTabValue((tab as QueryTab).name ?? "Query");
                          }
                        }}
                      >
                        {label}
                      </span>
                    )}
                    <button
                      type="button"
                      className="p-0.5 rounded hover:bg-muted-foreground/20"
                      onClick={(e) => {
                        e.stopPropagation();
                        if (editingQueryTabId === tab.id) setEditingQueryTabId(null);
                        tabs.closeTab(tab.id);
                        if (tab.type === "query") queryEngine.clearTabState(tab.id);
                        if (tab.type === "data") dataset.clearSourceView(tab.dataSourceId);
                      }}
                    >
                      <X className="size-3" />
                    </button>
                  </div>
                );
              })}
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={handleOpenAddSource}
              className="text-xs gap-1.5 h-8 shrink-0"
            >
              <Plus className="size-3.5" />
              Add Source
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => tabs.openQueryTab()}
              className="text-xs gap-1.5 h-8 shrink-0"
            >
              <TerminalSquare className="size-3.5" />
              New Query
            </Button>
          </div>
          <div className="flex items-center gap-1 shrink-0">
            {dataset.activeSource && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => {
                  setExportQuery(`SELECT * FROM ${dataset.activeSource!.qualified_name}`);
                  setShowExport(true);
                }}
                className="text-xs gap-1.5"
              >
                <Download className="size-3.5" />
                Export
              </Button>
            )}
          </div>
        </div>

        <div className="flex-1 overflow-hidden">
          {!hasTabs ? (
            <EmptyState onOpenAddSource={handleOpenAddSource} />
          ) : activeDataTab && activeDataSource ? (
            <DatasetViewer
              dataset={dataset.activeSource ?? activeDataSource}
              previewData={dataset.previewData}
              loading={dataset.loading}
              error={dataset.error}
              pagination={dataset.pagination}
              onPageChange={dataset.changePage}
              onPageSizeChange={dataset.changePageSize}
            />
          ) : activeQueryTab ? (
            <div className="flex flex-col h-full">
              <div className="h-2/5 min-h-[180px] border-b border-border">
                <QueryEditor
                  key={activeQueryTab.id}
                  initialSql={activeQueryTab.initialSql ?? querySql}
                  loading={queryEngine.loading}
                  dataSources={dataset.dataSources}
                  connectors={connectorsHook.connectors}
                  catalogs={connectorsHook.catalogs}
                  onExecute={(sql) =>
                    queryEngine.executeQuery(sql, {
                      useStreaming: settings.settings.streaming_enabled,
                      tabId: activeQueryTab.id,
                    })
                  }
                  onSqlChange={(sql) =>
                    tabs.updateTab(activeQueryTab.id, { initialSql: sql })
                  }
                />
              </div>
              <div className="flex-1 overflow-hidden">
                <ResultsTable
                  result={queryEngine.result}
                  loading={queryEngine.loading}
                  error={queryEngine.error}
                  executionTimeMs={queryEngine.executionTimeMs}
                  page={queryEngine.page}
                  pageSize={queryEngine.pageSize}
                  resultView={activeQueryTab.resultView ?? "table"}
                  onPageChange={queryEngine.changePage}
                  onPageSizeChange={queryEngine.changePageSize}
                  onResultViewChange={(view) =>
                    tabs.updateTab(activeQueryTab.id, { resultView: view })
                  }
                />
              </div>
            </div>
          ) : (
            <EmptyState onOpenAddSource={handleOpenAddSource} />
          )}
        </div>
      </main>

      <AddDataSourceModal
        open={showAddSource}
        onClose={() => {
          setShowAddSource(false);
          setDropFilePath(undefined);
        }}
        onCreated={handleDataSourceCreated}
        connectors={connectorsHook.connectors}
        onAddConnector={connectorsHook.addConnector}
        onTestConnector={connectorsHook.testConnection}
        initialFilePath={dropFilePath}
        onOpenNewConnection={() => setShowConnections(true)}
      />

      {showConnections && (
        <ConnectionManager
          onClose={() => setShowConnections(false)}
          onCreated={handleConnectionCreated}
        />
      )}

      {showExport && (
        <ExportModal
          defaultQuery={exportQuery || (dataset.activeSource ? `SELECT * FROM ${dataset.activeSource.qualified_name}` : "")}
          defaultFormat={
            ["csv", "parquet", "json"].includes(settings.settings.default_export_format)
              ? (settings.settings.default_export_format as "csv" | "parquet" | "json")
              : "csv"
          }
          onClose={() => setShowExport(false)}
        />
      )}

      {showProperties && (
        <PropertiesModal
          dataSource={showProperties}
          connector={connectorsHook.getConnectorById(showProperties.connector_id)}
          onClose={() => setShowProperties(null)}
        />
      )}

      {showImportFor && (
        <AddDataSourceModal
          open={!!showImportFor}
          onClose={() => setShowImportFor(null)}
          onCreated={() => {}}
          onUpdated={(ds) => {
            dataset.updateDataSource(ds);
            setShowImportFor(null);
          }}
          connectors={connectorsHook.connectors}
          onAddConnector={connectorsHook.addConnector}
          onTestConnector={connectorsHook.testConnection}
          initialFilePath={undefined}
          onOpenNewConnection={() => setShowConnections(true)}
          existingDataSource={showImportFor}
        />
      )}

      {showSettings && (
        <SettingsModal
          open={showSettings}
          onClose={() => setShowSettings(false)}
          settings={settings.settings}
          onUpdate={settings.updateSettings}
        />
      )}

      {showAddFromUrl && (
        <AddFromUrlDialog
          onClose={() => setShowAddFromUrl(false)}
          onCreated={(ds) => {
            dataset.addDataSource(ds);
            tabs.openDataTab(ds);
            setShowAddFromUrl(false);
          }}
          onAddConnector={connectorsHook.addConnector}
        />
      )}

      <CreateGraphModal
        open={showCreateGraph}
        onOpenChange={setShowCreateGraph}
        dataSources={dataset.dataSources}
        onCreated={refreshGraphs}
      />

      {showAlgorithm && (
        <AlgorithmPanel
          open={!!showAlgorithm}
          onOpenChange={(open: boolean) => { if (!open) setShowAlgorithm(null); }}
          graphName={showAlgorithm}
          propertyGraphs={propertyGraphs}
        />
      )}

      <EtlJobModal
        open={showEtlModal}
        onClose={() => setShowEtlModal(false)}
        connectors={connectorsHook.connectors}
        onAddConnector={connectorsHook.addConnector}
        onSubmit={handleEtlSubmit}
      />

      <EtlProgress
        open={showEtlProgress}
        onClose={() => setShowEtlProgress(false)}
        job={activeEtlJob}
        connectors={connectorsHook.connectors}
        activeProgress={etlHook.activeProgress}
        lastComplete={etlHook.lastComplete}
        onCancel={etlHook.cancelJob}
      />
    </div>
  );
}

function AddFromUrlDialog({
  onClose,
  onCreated,
  onAddConnector,
}: {
  onClose: () => void;
  onCreated: (ds: DataSource) => void;
  onAddConnector: (params: {
    name: string;
    connectorType: "local_file";
    config: { path: string; format?: string };
  }) => Promise<AddConnectorResult>;
}) {
  const [url, setUrl] = useState("");
  const [format, setFormat] = useState("csv");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const inferFormatFromUrl = (u: string) => {
    const pathname = u.split("?")[0];
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
    return map[ext] ?? "csv";
  };

  const handleUrlChange = (value: string) => {
    setUrl(value);
    if (value) setFormat(inferFormatFromUrl(value));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const u = url.trim();
    if (!u) return;
    setLoading(true);
    setError(null);
    try {
      const name = u.split("/").pop()?.split("?")[0] ?? "url_source";
      const viewName = name.replace(/\.[^.]+$/, "").replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase() || "url_source";
      const { connector: conn } = await onAddConnector({
        name,
        connectorType: "local_file",
        config: { path: u, format },
      });
      const ds = await createDataSourceIpc({
        name,
        viewName,
        connectorId: conn.id,
      });
      onCreated(ds);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load from URL");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Data Source from URL</DialogTitle>
          <DialogDescription>
            Load a file from an HTTP(S) URL. DuckDB will fetch it (e.g. via httpfs). Supports CSV, JSON, Parquet, and other formats.
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="url">URL</Label>
            <Input
              id="url"
              type="url"
              placeholder="https://example.com/data.csv"
              value={url}
              onChange={(e) => handleUrlChange(e.target.value)}
              disabled={loading}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="format">Format</Label>
            <Select value={format} onValueChange={setFormat} disabled={loading}>
              <SelectTrigger id="format">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {["csv", "tsv", "json", "jsonl", "parquet", "xlsx", "avro"].map((f) => (
                  <SelectItem key={f} value={f}>
                    {f.toUpperCase()}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          {error && (
            <p className="text-sm text-destructive">{error}</p>
          )}
          <div className="flex justify-end gap-2">
            <Button type="button" variant="outline" onClick={onClose} disabled={loading}>
              Cancel
            </Button>
            <Button type="submit" disabled={loading || !url.trim()}>
              {loading ? "Loading…" : "Add"}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function EmptyState({ onOpenAddSource }: { onOpenAddSource: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center h-full gap-4">
      <div className="rounded-xl border border-dashed border-border p-8 flex flex-col items-center gap-4 max-w-sm text-center">
        <div className="rounded-full bg-muted p-3">
          <Table2 className="size-8 text-muted-foreground" />
        </div>
        <div>
          <h2 className="text-base font-semibold text-foreground mb-1">
            No tab open
          </h2>
          <p className="text-sm text-muted-foreground">
            Click a data source in the sidebar or add a new one to get started
          </p>
        </div>
        <Button onClick={onOpenAddSource} className="gap-2">
          <FolderOpen className="size-4" />
          Add Data Source
        </Button>
        <p className="text-xs text-muted-foreground">
          Supports CSV, TSV, JSON, JSONL, Parquet, Avro, Arrow IPC, SQLite, PostgreSQL
        </p>
      </div>
    </div>
  );
}

function PropertiesModal({
  dataSource,
  connector,
  onClose,
}: {
  dataSource: DataSource;
  connector?: Connector;
  onClose: () => void;
}) {
  const schema = dataSource?.schema ?? [];
  const connectorLabel = connector
    ? connector.connector_type === "local_file"
      ? connector.config.format?.toUpperCase() ?? "File"
      : connector.connector_type.replace(/_/g, " ").toUpperCase()
    : "Unknown";
  return (
    <Dialog open={!!dataSource} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="w-[95vw] max-w-6xl sm:max-w-6xl min-h-[60vh] max-h-[90vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Properties</DialogTitle>
          <DialogDescription>{dataSource?.name}</DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 text-sm overflow-hidden flex-1 min-h-0">
          <div className="grid grid-cols-[100px_1fr] gap-2 shrink-0">
            <span className="text-muted-foreground">Name</span>
            <span className="font-mono">{dataSource?.name}</span>
            <span className="text-muted-foreground">Qualified name</span>
            <span className="font-mono">{dataSource?.qualified_name}</span>
            {dataSource?.view_name && (
              <>
                <span className="text-muted-foreground">View name</span>
                <span className="font-mono">{dataSource.view_name}</span>
              </>
            )}
            <span className="text-muted-foreground">Source</span>
            <span>{connectorLabel}</span>
            <span className="text-muted-foreground">Kind</span>
            <span>{dataSource?.kind}</span>
            <span className="text-muted-foreground">Rows</span>
            <span>{dataSource?.row_count?.toLocaleString() ?? "?"}</span>
          </div>
          <div className="flex flex-col min-h-0">
            <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2 shrink-0">
              Columns and types
            </h4>
            {schema.length > 0 ? (
              <div className="border border-border rounded-md overflow-auto shrink min-h-0">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-muted/50">
                      <th className="text-left font-medium px-3 py-2">Column</th>
                      <th className="text-left font-medium px-3 py-2">Type</th>
                      <th className="text-left font-medium px-3 py-2">Nullable</th>
                    </tr>
                  </thead>
                  <tbody>
                    {schema.map((col) => (
                      <tr key={col.name} className="border-b border-border last:border-0">
                        <td className="font-mono px-3 py-1.5">{col.name}</td>
                        <td className="font-mono px-3 py-1.5 text-muted-foreground">{col.data_type}</td>
                        <td className="px-3 py-1.5">{col.nullable ? "Yes" : "No"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="text-muted-foreground text-xs italic py-2">No column information available.</p>
            )}
          </div>
        </div>
        <div className="flex justify-end shrink-0 pt-2">
          <Button variant="outline" size="sm" onClick={onClose}>
            Close
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
