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
import { useDataset } from "@/hooks/useDataset";
import { useSettings } from "@/hooks/useSettings";
import { useQueryEngine } from "@/hooks/useQuery";
import { useWorkspaceTabs } from "@/hooks/useWorkspaceTabs";
import type { DataTab, QueryTab, ViewMode, WorkspaceTab } from "@/hooks/useWorkspaceTabs";
import {
  listDataSources,
  listConnections,
  getPersistedTabs,
  setPersistedTabs,
  removeDataSource as removeDataSourceIpc,
  removeConnection as removeConnectionIpc,
} from "@/lib/ipc";
import type { DataSource, ConnectionInfo } from "@/lib/types";

const SIDEBAR_MIN = 180;
const SIDEBAR_MAX = 480;

export function AppLayout() {
  const dataset = useDataset();
  const settings = useSettings();
  const queryEngine = useQueryEngine();
  const tabs = useWorkspaceTabs();
  const [showAddSource, setShowAddSource] = useState(false);
  const [showConnections, setShowConnections] = useState(false);
  const [showExport, setShowExport] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [exportQuery, setExportQuery] = useState("");
  const [querySql, setQuerySql] = useState("");
  const [connections, setConnections] = useState<ConnectionInfo[]>([]);
  const [dropFilePath, setDropFilePath] = useState<string | undefined>();
  const [showProperties, setShowProperties] = useState<DataSource | null>(null);
  const [showImportFor, setShowImportFor] = useState<DataSource | null>(null);
  const [editingQueryTabId, setEditingQueryTabId] = useState<string | null>(null);
  const [editingTabValue, setEditingTabValue] = useState("");
  const lastActiveDataTabRef = useRef<{ tabId: string; dataSourceId: string; viewMode?: ViewMode } | null>(null);
  const hasHydratedTabsRef = useRef(false);

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

  useEffect(() => {
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
    listConnections()
      .then((conns) => setConnections(conns))
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
      tabs.openQueryTab(`SELECT * FROM "${ds.view_name}" LIMIT 100`);
      setQuerySql(`SELECT * FROM "${ds.view_name}" LIMIT 100`);
    },
    [tabs]
  );

  function buildViewSql(ds: DataSource, viewMode: ViewMode): string {
    const v = ds.view_name;
    const total = ds.row_count ?? 0;
    switch (viewMode) {
      case "first100":
        return `SELECT * FROM "${v}" LIMIT 100`;
      case "last100": {
        const offset = Math.max(0, total - 100);
        return `SELECT * FROM "${v}" LIMIT 100 OFFSET ${offset}`;
      }
      case "all":
        return `SELECT * FROM "${v}" LIMIT 10000`;
      case "filtered":
      default:
        return `SELECT * FROM "${v}" LIMIT 100`;
    }
  }

  const handleViewDataAsQuery = useCallback(
    (ds: DataSource, viewMode: ViewMode) => {
      const ext = ds.format?.trim().toLowerCase() || (ds.path?.match(/\.([a-z0-9]+)$/i)?.[1]?.toLowerCase() ?? "data");
      const tabName = `${ds.name}[${ext}]`;
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
    (ds: DataSource) => {
      dataset.addDataSource(ds);
      tabs.openDataTab(ds);
    },
    [dataset, tabs]
  );

  const handleConnectionCreated = useCallback((conn: ConnectionInfo) => {
    setConnections((prev) => [...prev, conn]);
  }, []);

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

  const handleRemoveConnection = useCallback(async (id: string) => {
    try {
      await removeConnectionIpc(id);
      setConnections((prev) => prev.filter((c) => c.id !== id));
    } catch {
      // silently fail
    }
  }, []);

  const handleOpenAddSource = useCallback(() => {
    setDropFilePath(undefined);
    setShowAddSource(true);
  }, []);

  const handleExport = useCallback((ds: DataSource) => {
    setExportQuery(`SELECT * FROM "${ds.view_name}"`);
    setShowExport(true);
  }, []);

  const handleImport = useCallback((ds: DataSource) => {
    setShowImportFor(ds);
  }, []);

  const handleProperties = useCallback((ds: DataSource) => {
    setShowProperties(ds);
  }, []);

  const hasTabs = tabs.openTabs.length > 0;

  return (
    <div className="flex h-screen bg-background text-foreground">
      <div
        className="flex shrink-0 flex-col h-full"
        style={{ width: sidebarWidth, minWidth: sidebarWidth }}
      >
        <Sidebar
        dataSources={dataset.dataSources}
        connections={connections}
        activeSourceId={dataset.activeSource?.id ?? null}
        queryHistory={queryEngine.history}
        onAddDataSource={handleOpenAddSource}
        onDataSourceSelect={handleDataSourceSelect}
        onDataSourceRemove={handleRemoveDataSource}
        onAddConnection={() => setShowConnections(true)}
        onConnectionRemove={handleRemoveConnection}
        onQuerySelect={handleQuerySelect}
        onNewQuery={handleNewQuery}
        onOpenDataTab={handleDataSourceSelect}
        onViewDataAsQuery={handleViewDataAsQuery}
        onExport={handleExport}
        onImport={handleImport}
        onProperties={handleProperties}
        onOpenSettings={() => setShowSettings(true)}
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
        {/* Tab bar + toolbar */}
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
                  setExportQuery(`SELECT * FROM "${dataset.activeSource!.view_name}"`);
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

        {/* Content */}
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
                  onPageChange={queryEngine.changePage}
                  onPageSizeChange={queryEngine.changePageSize}
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
        connections={connections}
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
          defaultQuery={exportQuery || (dataset.activeSource ? `SELECT * FROM "${dataset.activeSource.view_name}"` : "")}
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
          connections={connections}
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
    </div>
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
          Supports CSV, TSV, JSON, JSONL, Parquet, Arrow IPC
        </p>
      </div>
    </div>
  );
}

function PropertiesModal({
  dataSource,
  onClose,
}: {
  dataSource: DataSource;
  onClose: () => void;
}) {
  const schema = dataSource?.schema ?? [];
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
            <span className="text-muted-foreground">View name</span>
            <span className="font-mono">{dataSource?.view_name}</span>
            <span className="text-muted-foreground">Path</span>
            <span className="font-mono break-all">{dataSource?.path}</span>
            <span className="text-muted-foreground">Format</span>
            <span>{dataSource?.format}</span>
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

