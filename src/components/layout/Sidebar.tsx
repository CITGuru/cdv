import { useState, useCallback, useMemo } from "react";
import {
  Plus,
  Database,
  CircleDot,
  TerminalSquare,
  Upload,
  FolderOpen,
  Settings,
  History,
  Link,
  Network,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type {
  DataSource,
  Connector,
  CatalogEntry,
  ConnectorBrowseCache,
  PropertyGraphInfo,
  EtlJob,
  EtlProgressEvent,
} from "@/lib/types";
import type { QueryHistoryEntry } from "@/hooks/useQuery";
import type { ViewMode } from "@/hooks/useWorkspaceTabs";
import { EtlJobList } from "@/components/etl/EtlJobList";
import {
  DataSourceTreeItem,
  DatabaseConnectorItem,
  CloudConnectorItem,
  GraphTreeItem,
} from "@/components/sidebar";

type SidebarView = "sources" | "history" | "etl";

interface SidebarProps {
  dataSources: DataSource[];
  connectors: Connector[];
  catalogs: Record<string, CatalogEntry[]>;
  connectorBrowse?: Record<string, ConnectorBrowseCache>;
  catalogLoading?: Record<string, boolean>;
  dbListLoading?: Record<string, boolean>;
  activeSourceId: string | null;
  queryHistory: QueryHistoryEntry[];
  propertyGraphs: PropertyGraphInfo[];
  graphSupported: boolean;
  onAddDataSource: () => void;
  onOpenQueryConsole?: () => void;
  onAddFolder?: () => void;
  onAddFromUrl?: () => void;
  onCreateGraph?: () => void;
  onDropGraph?: (name: string) => void;
  onGraphQuery?: (graphName: string) => void;
  onRunAlgorithm?: (graphName: string) => void;
  onDataSourceSelect: (ds: DataSource, viewMode?: ViewMode) => void;
  onDataSourceRemove: (id: string) => void;
  onConnectorRemove: (id: string) => void;
  onConnectorRefresh?: (id: string) => void;
  onConnectorExpand?: (connectorId: string) => void;
  onDataSourceRefresh?: (ds: DataSource) => void;
  onQuerySelect: (sql: string) => void;
  onNewQuery?: (ds: DataSource) => void;
  onOpenDataTab?: (ds: DataSource, viewMode?: ViewMode) => void;
  onViewDataAsQuery?: (ds: DataSource, viewMode: ViewMode) => void;
  onExport?: (ds: DataSource) => void;
  onImport?: (ds: DataSource) => void;
  onProperties?: (ds: DataSource) => void;
  onOpenSettings?: () => void;
  onImportDbTable?: (
    connectorId: string,
    database: string | undefined,
    schema: string,
    table: string
  ) => void;
  onConnectDatabase?: (connectorId: string, database: string) => Promise<void>;
  onNewQueryFromTable?: (qualifiedName: string) => void;
  etlJobs?: EtlJob[];
  etlActiveProgress?: EtlProgressEvent | null;
  onCreateEtlJob?: () => void;
  onRunEtlJob?: (jobId: string) => void;
  onCancelEtlJob?: () => void;
  onDeleteEtlJob?: (jobId: string) => void;
  onViewEtlProgress?: (jobId: string) => void;
}

export function Sidebar({
  dataSources,
  connectors,
  catalogs,
  connectorBrowse,
  catalogLoading,
  dbListLoading,
  activeSourceId,
  queryHistory,
  propertyGraphs,
  graphSupported,
  onAddDataSource,
  onOpenQueryConsole,
  onAddFolder,
  onAddFromUrl,
  onCreateGraph,
  onDropGraph,
  onGraphQuery,
  onRunAlgorithm,
  onDataSourceSelect,
  onDataSourceRemove,
  onConnectorRemove,
  onConnectorRefresh,
  onConnectorExpand,
  onDataSourceRefresh,
  onQuerySelect,
  onNewQuery,
  onOpenDataTab,
  onViewDataAsQuery,
  onExport,
  onImport,
  onProperties,
  onOpenSettings,
  onImportDbTable,
  onConnectDatabase,
  onNewQueryFromTable,
  etlJobs,
  etlActiveProgress,
  onCreateEtlJob,
  onRunEtlJob,
  onCancelEtlJob,
  onDeleteEtlJob,
  onViewEtlProgress,
}: SidebarProps) {
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [activeView, setActiveView] = useState<SidebarView>("sources");

  const toggleExpanded = useCallback((id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const isIdExpanded = useCallback((id: string) => expandedIds.has(id), [expandedIds]);

  const treeEntries = useMemo(() => {
    return connectors.map((conn) => {
      const dsForConn = dataSources.filter((ds) => ds.connector_id === conn.id);
      const type = conn.connector_type;
      if (type === "local_file") {
        return { kind: "file" as const, connector: conn, dataSources: dsForConn };
      } else if (["sqlite", "duckdb", "postgresql", "snowflake", "ducklake"].includes(type)) {
        return { kind: "db" as const, connector: conn, dataSources: dsForConn };
      } else {
        return { kind: "cloud" as const, connector: conn, dataSources: dsForConn };
      }
    });
  }, [connectors, dataSources]);

  return (
    <aside className="w-full bg-sidebar flex flex-col h-full min-w-0">
      {/* Header */}
      <div className="flex items-center gap-2 h-10 px-4 border-b border-border shrink-0">
        <Database className="size-4 text-primary shrink-0" />
        <h1 className="text-sm font-semibold text-sidebar-foreground tracking-tight truncate">
          Columnar Data Viewer
        </h1>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-0.5 px-2 py-1.5 border-b border-border shrink-0">
        <DropdownMenu>
          <DropdownMenuTrigger
            className="inline-flex shrink-0 items-center justify-center rounded-lg h-7 w-7 text-muted-foreground hover:text-sidebar-foreground hover:bg-sidebar-accent/50 outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <Plus className="size-4" />
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="min-w-52">
            {onOpenQueryConsole && (
              <DropdownMenuItem onSelect={onOpenQueryConsole}>
                <TerminalSquare className="size-4" />
                Query Console
              </DropdownMenuItem>
            )}
            <DropdownMenuItem onSelect={onAddDataSource}>
              <Database className="size-4" />
              Data Source
            </DropdownMenuItem>
            {onAddFolder && (
              <DropdownMenuItem onSelect={onAddFolder}>
                <FolderOpen className="size-4" />
                Folder
              </DropdownMenuItem>
            )}
            {onAddFromUrl && (
              <DropdownMenuItem onSelect={onAddFromUrl}>
                <Link className="size-4" />
                Data Source from URL
              </DropdownMenuItem>
            )}
            {onCreateEtlJob && (
              <DropdownMenuItem onSelect={onCreateEtlJob}>
                <Upload className="size-4" />
                ETL Job
              </DropdownMenuItem>
            )}
            {onCreateGraph && (
              <DropdownMenuItem onSelect={onCreateGraph}>
                <Network className="size-4" />
                Property Graph
              </DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>

        <div className="w-px h-4 bg-border mx-0.5" />

        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant={activeView === "sources" ? "secondary" : "ghost"}
              size="icon-xs"
              onClick={() => setActiveView("sources")}
              className="h-7 w-7"
            >
              <Database className="size-4" />
            </Button>
          </TooltipTrigger>
          <TooltipContent side="bottom">Data sources</TooltipContent>
        </Tooltip>

        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant={activeView === "history" ? "secondary" : "ghost"}
              size="icon-xs"
              onClick={() => setActiveView("history")}
              className="h-7 w-7"
            >
              <History className="size-4" />
            </Button>
          </TooltipTrigger>
          <TooltipContent side="bottom">Query history</TooltipContent>
        </Tooltip>

        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant={activeView === "etl" ? "secondary" : "ghost"}
              size="icon-xs"
              onClick={() => setActiveView("etl")}
              className="h-7 w-7 relative"
            >
              <Upload className="size-4" />
              {etlJobs && etlJobs.some((j) => j.status === "running") && (
                <span className="absolute top-0.5 right-0.5 size-1.5 rounded-full bg-blue-500" />
              )}
            </Button>
          </TooltipTrigger>
          <TooltipContent side="bottom">ETL Jobs</TooltipContent>
        </Tooltip>
      </div>

      {/* Body */}
      <ScrollArea className="flex-1 min-h-0 overflow-hidden">
        {activeView === "sources" ? (
          <div className="py-1.5 px-1">
            {treeEntries.length === 0 ? (
              <p className="px-3 py-8 text-xs text-muted-foreground text-center italic">
                No data sources
              </p>
            ) : (
              <div className="space-y-0.5">
                {treeEntries.flatMap((entry) => {
                  if (entry.kind === "file") {
                    return entry.dataSources.map((ds) => (
                      <DataSourceTreeItem
                        key={ds.id}
                        source={ds}
                        connector={entry.connector}
                        isActive={activeSourceId === ds.id}
                        isExpanded={expandedIds.has(ds.id)}
                        onToggleExpand={() => toggleExpanded(ds.id)}
                        onSelect={onDataSourceSelect}
                        onRemove={onDataSourceRemove}
                        onRefresh={onDataSourceRefresh}
                        onNewQuery={onNewQuery}
                        onOpenDataTab={onOpenDataTab}
                        onViewDataAsQuery={onViewDataAsQuery}
                        onExport={onExport}
                        onImport={onImport}
                        onProperties={onProperties}
                      />
                    ));
                  } else if (entry.kind === "db") {
                    return [
                      <DatabaseConnectorItem
                        key={entry.connector.id}
                        connector={entry.connector}
                        browse={connectorBrowse?.[entry.connector.id]}
                        catalog={catalogs[entry.connector.id] ?? []}
                        catalogIsLoading={catalogLoading?.[entry.connector.id] ?? false}
                        databaseListLoading={dbListLoading?.[entry.connector.id] ?? false}
                        dataSources={dataSources}
                        isExpanded={expandedIds.has(`db-${entry.connector.id}`)}
                        onToggleExpand={() => {
                          const key = `db-${entry.connector.id}`;
                          const isExpanding = !expandedIds.has(key);
                          toggleExpanded(key);
                          if (isExpanding && onConnectorExpand) {
                            onConnectorExpand(entry.connector.id);
                          }
                        }}
                        isIdExpanded={isIdExpanded}
                        onToggleSchemaExpand={toggleExpanded}
                        onRemove={onConnectorRemove}
                        onRefresh={onConnectorRefresh}
                        onImportTable={onImportDbTable}
                        onConnectDatabase={onConnectDatabase}
                        onNewQuery={onNewQueryFromTable}
                        onDataSourceSelect={onDataSourceSelect}
                        activeSourceId={activeSourceId}
                      />,
                    ];
                  } else {
                    return [
                      <CloudConnectorItem
                        key={entry.connector.id}
                        connector={entry.connector}
                        onRemove={onConnectorRemove}
                      />,
                    ];
                  }
                })}
              </div>
            )}

            {/* Property Graphs section */}
            {graphSupported && propertyGraphs.length > 0 && (
              <div className="mt-2">
                <div className="flex items-center gap-1.5 px-3 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">
                  <Network className="size-3 shrink-0" />
                  Graphs
                </div>
                <div className="space-y-0.5">
                  {propertyGraphs.map((graph) => (
                    <GraphTreeItem
                      key={graph.name}
                      graph={graph}
                      isExpanded={expandedIds.has(`graph-${graph.name}`)}
                      onToggleExpand={() => toggleExpanded(`graph-${graph.name}`)}
                      onNewQuery={onGraphQuery}
                      onRunAlgorithm={onRunAlgorithm}
                      onDrop={onDropGraph}
                    />
                  ))}
                </div>
              </div>
            )}

          </div>
        ) : activeView === "history" ? (
          <div className="py-1.5 px-1">
            {queryHistory.length === 0 ? (
              <p className="px-3 py-8 text-xs text-muted-foreground text-center italic">
                No queries yet
              </p>
            ) : (
              <div className="space-y-0.5">
                {queryHistory.slice(0, 20).map((entry, i) => (
                  <Tooltip key={i}>
                    <TooltipTrigger asChild>
                      <button
                        onClick={() => onQuerySelect(entry.sql)}
                        className="w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-xs text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground transition-colors min-h-7"
                      >
                        <CircleDot
                          className={`size-2.5 shrink-0 ${
                            entry.status === "success"
                              ? "text-green-500"
                              : "text-destructive"
                          }`}
                        />
                        <span className="truncate font-mono text-left">{entry.sql}</span>
                      </button>
                    </TooltipTrigger>
                    <TooltipContent side="right" className="max-w-sm">
                      <pre className="text-xs font-mono whitespace-pre-wrap">
                        {entry.sql}
                      </pre>
                    </TooltipContent>
                  </Tooltip>
                ))}
              </div>
            )}
          </div>
        ) : (
          /* ETL Jobs view */
          <div className="py-1.5 px-1">
            {onCreateEtlJob && (
              <div className="px-2 mb-2">
                <Button
                  variant="outline"
                  size="sm"
                  className="w-full justify-start gap-2 text-xs"
                  onClick={onCreateEtlJob}
                >
                  <Plus className="size-3.5" />
                  New ETL Job
                </Button>
              </div>
            )}
            {!etlJobs || etlJobs.length === 0 ? (
              <div className="px-3 py-8 text-center">
                <Upload className="size-8 text-muted-foreground/40 mx-auto mb-2" />
                <p className="text-xs text-muted-foreground italic">
                  No ETL jobs yet
                </p>
                <p className="text-[10px] text-muted-foreground/70 mt-1">
                  Migrate data from PostgreSQL to DuckLake on R2
                </p>
              </div>
            ) : (
              <EtlJobList
                jobs={etlJobs}
                connectors={connectors}
                activeProgress={etlActiveProgress ?? null}
                onRunJob={onRunEtlJob ?? (() => {})}
                onCancelJob={onCancelEtlJob ?? (() => {})}
                onDeleteJob={onDeleteEtlJob ?? (() => {})}
                onViewProgress={onViewEtlProgress ?? (() => {})}
              />
            )}
          </div>
        )}
      </ScrollArea>

      {/* Footer */}
      {onOpenSettings && (
        <div className="shrink-0 border-t border-border p-1.5">
          <Button
            variant="ghost"
            size="sm"
            className="w-full justify-start gap-2 text-muted-foreground hover:text-sidebar-foreground"
            onClick={onOpenSettings}
          >
            <Settings className="size-4 shrink-0" />
            <span className="text-xs">Settings</span>
          </Button>
        </div>
      )}
    </aside>
  );
}
