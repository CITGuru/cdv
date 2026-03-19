import { useState, useCallback, useMemo } from "react";
import type { ComponentType, SVGProps } from "react";
import {
  FileSpreadsheet,
  FileText,
  FileJson,
  Plus,
  Database,
  CircleDot,
  Trash2,
  Table2,
  TerminalSquare,
  Download,
  Upload,
  Info,
  ChevronRight,
  ChevronDown,
  FolderOpen,
  Folder,
  LayoutGrid,
  Settings,
  RefreshCw,
  History,
  Link,
  Network,
  FlaskConical,
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
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuSub,
  ContextMenuSubContent,
  ContextMenuSubTrigger,
  ContextMenuTrigger,
} from "@/components/ui/context-menu";
import type { DataSource, Connector, CatalogEntry, ColumnInfo, PropertyGraphInfo } from "@/lib/types";
import type { QueryHistoryEntry } from "@/hooks/useQuery";
import type { ViewMode } from "@/hooks/useWorkspaceTabs";

type IconComponent = ComponentType<SVGProps<SVGSVGElement> & { className?: string }>;

function getDataSourceIcon(_source: DataSource, connector?: Connector): IconComponent {
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

function getConnectorIcon(connector: Connector): IconComponent {
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

function getConnectorLabel(connector: Connector): string {
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

function getConnectorIconColor(connector: Connector): string {
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

function getFileIconColor(connector?: Connector): string {
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

type SidebarView = "sources" | "history";

interface SidebarProps {
  dataSources: DataSource[];
  connectors: Connector[];
  catalogs: Record<string, CatalogEntry[]>;
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
  onDataSourceRefresh?: (ds: DataSource) => void;
  onQuerySelect: (sql: string) => void;
  onNewQuery?: (ds: DataSource) => void;
  onOpenDataTab?: (ds: DataSource, viewMode?: ViewMode) => void;
  onViewDataAsQuery?: (ds: DataSource, viewMode: ViewMode) => void;
  onExport?: (ds: DataSource) => void;
  onImport?: (ds: DataSource) => void;
  onProperties?: (ds: DataSource) => void;
  onOpenSettings?: () => void;
  onImportDbTable?: (connectorId: string, schema: string, table: string) => void;
  onNewQueryFromTable?: (qualifiedName: string) => void;
}

export function Sidebar({
  dataSources,
  connectors,
  catalogs,
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
  onNewQueryFromTable,
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
                        catalog={catalogs[entry.connector.id] ?? []}
                        dataSources={dataSources}
                        isExpanded={expandedIds.has(`db-${entry.connector.id}`)}
                        onToggleExpand={() => toggleExpanded(`db-${entry.connector.id}`)}
                        expandedIds={expandedIds}
                        onToggleSchemaExpand={(id) => toggleExpanded(id)}
                        onRemove={onConnectorRemove}
                        onRefresh={onConnectorRefresh}
                        onImportTable={onImportDbTable}
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
        ) : (
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

function DataSourceTreeItem({
  source,
  connector,
  isActive,
  isExpanded,
  onToggleExpand,
  onSelect,
  onRemove,
  onRefresh,
  onNewQuery,
  onOpenDataTab,
  onViewDataAsQuery,
  onExport,
  onImport,
  onProperties,
}: {
  source: DataSource;
  connector?: Connector;
  isActive: boolean;
  isExpanded: boolean;
  onToggleExpand: () => void;
  onSelect: (ds: DataSource, viewMode?: ViewMode) => void;
  onRemove: (id: string) => void;
  onRefresh?: (ds: DataSource) => void;
  onNewQuery?: (ds: DataSource) => void;
  onOpenDataTab?: (ds: DataSource, viewMode?: ViewMode) => void;
  onViewDataAsQuery?: (ds: DataSource, viewMode: ViewMode) => void;
  onExport?: (ds: DataSource) => void;
  onImport?: (ds: DataSource) => void;
  onProperties?: (ds: DataSource) => void;
}) {
  const openOrFocus = (viewMode?: ViewMode) => {
    if (onOpenDataTab) onOpenDataTab(source, viewMode);
    else onSelect(source, viewMode);
  };
  const onViewData = (viewMode: ViewMode) => {
    if (onViewDataAsQuery) onViewDataAsQuery(source, viewMode);
    else openOrFocus(viewMode);
  };
  const schema = source.schema ?? [];
  const hasColumns = schema.length > 0;
  const columnCount = schema.length;

  return (
    <div className="flex flex-col gap-0 rounded-md">
      <ContextMenu>
        <ContextMenuTrigger asChild>
          <div
            className={`group flex items-center gap-0.5 rounded-md transition-colors min-h-7 ${
              isActive
                ? "bg-sidebar-accent text-sidebar-accent-foreground"
                : "text-sidebar-foreground hover:bg-sidebar-accent/50"
            }`}
          >
            <button
              type="button"
              aria-label={isExpanded ? "Collapse" : "Expand"}
              className="shrink-0 p-0.5 rounded hover:bg-sidebar-accent/80 flex items-center justify-center w-5"
              onClick={(e) => {
                e.stopPropagation();
                onToggleExpand();
              }}
            >
              {hasColumns ? (
                isExpanded ? (
                  <ChevronDown className="size-3.5 text-muted-foreground" />
                ) : (
                  <ChevronRight className="size-3.5 text-muted-foreground" />
                )
              ) : (
                <span className="inline-block w-3" aria-hidden />
              )}
            </button>
            <button
              onClick={() => onSelect(source)}
              className="flex-1 flex items-center gap-2 px-1.5 py-1 min-w-0 text-left"
            >
              {(() => {
                const Icon = getDataSourceIcon(source, connector);
                const color = getFileIconColor(connector);
                return <Icon className={`size-4 shrink-0 ${color}`} />;
              })()}
              <span className="truncate text-xs font-medium flex-1 text-left">
                {source.name}
              </span>
              {hasColumns && (
                <span className="shrink-0 text-[10px] text-muted-foreground tabular-nums px-1">
                  {columnCount}
                </span>
              )}
            </button>
            <Button
              variant="ghost"
              size="icon-xs"
              className="opacity-0 group-hover:opacity-100 shrink-0 h-6 w-6"
              onClick={(e) => {
                e.stopPropagation();
                onRemove(source.id);
              }}
            >
              <Trash2 className="size-3 text-muted-foreground hover:text-destructive" />
            </Button>
          </div>
        </ContextMenuTrigger>
        <ContextMenuContent className="w-52">
          {onNewQuery && (
            <ContextMenuItem onSelect={() => onNewQuery(source)}>
              <TerminalSquare className="size-4 mr-2" />
              New Query
            </ContextMenuItem>
          )}
          {(onOpenDataTab || onViewDataAsQuery) && (
            <ContextMenuSub>
              <ContextMenuSubTrigger>
                <Table2 className="size-4 mr-2" />
                View Data
              </ContextMenuSubTrigger>
              <ContextMenuSubContent>
                <ContextMenuItem onSelect={() => onViewData("first100")}>
                  First 100 Rows
                </ContextMenuItem>
                <ContextMenuItem onSelect={() => onViewData("last100")}>
                  Last 100 Rows
                </ContextMenuItem>
                <ContextMenuItem onSelect={() => onViewData("all")}>
                  All Rows
                </ContextMenuItem>
                <ContextMenuItem onSelect={() => onViewData("filtered")}>
                  Filtered Rows
                </ContextMenuItem>
              </ContextMenuSubContent>
            </ContextMenuSub>
          )}
          {onRefresh && (
            <ContextMenuItem onSelect={() => onRefresh(source)}>
              <RefreshCw className="size-4 mr-2" />
              Refresh
            </ContextMenuItem>
          )}
          <ContextMenuSeparator />
          <ContextMenuItem
            variant="destructive"
            onSelect={() => onRemove(source.id)}
          >
            <Trash2 className="size-4 mr-2" />
            Drop
          </ContextMenuItem>
          {onExport && (
            <ContextMenuItem onSelect={() => onExport(source)}>
              <Download className="size-4 mr-2" />
              Export
            </ContextMenuItem>
          )}
          {onImport && (
            <ContextMenuItem onSelect={() => onImport(source)}>
              <Upload className="size-4 mr-2" />
              Import
            </ContextMenuItem>
          )}
          {onProperties && (
            <>
              <ContextMenuSeparator />
              <ContextMenuItem onSelect={() => onProperties(source)}>
                <Info className="size-4 mr-2" />
                Properties
              </ContextMenuItem>
            </>
          )}
        </ContextMenuContent>
      </ContextMenu>
      {hasColumns && isExpanded && (
        <div className="ml-2 pl-4 border-l border-border/50 flex flex-col py-0.5">
          <div className="flex items-center gap-1.5 py-1 px-1.5 text-[11px] font-medium text-muted-foreground uppercase tracking-wider">
            <FolderOpen className="size-3.5 shrink-0" />
            <span>Columns {columnCount}</span>
          </div>
          {schema.map((col) => (
            <ColumnTreeRow key={col.name} column={col} />
          ))}
        </div>
      )}
    </div>
  );
}

function DatabaseConnectorItem({
  connector,
  catalog,
  dataSources,
  isExpanded,
  onToggleExpand,
  expandedIds,
  onToggleSchemaExpand,
  onRemove,
  onRefresh,
  onImportTable,
  onNewQuery,
  onDataSourceSelect,
  activeSourceId,
}: {
  connector: Connector;
  catalog: CatalogEntry[];
  dataSources: DataSource[];
  isExpanded: boolean;
  onToggleExpand: () => void;
  expandedIds: Set<string>;
  onToggleSchemaExpand: (id: string) => void;
  onRemove: (id: string) => void;
  onRefresh?: (id: string) => void;
  onImportTable?: (connectorId: string, schema: string, table: string) => void;
  onNewQuery?: (qualifiedName: string) => void;
  onDataSourceSelect?: (ds: DataSource, viewMode?: ViewMode) => void;
  activeSourceId: string | null;
}) {
  const Icon = getConnectorIcon(connector);
  const label = getConnectorLabel(connector);
  const iconColor = getConnectorIconColor(connector);

  const schemas = new Map<string, CatalogEntry[]>();
  for (const entry of catalog) {
    const s = entry.schema ?? "default";
    if (!schemas.has(s)) schemas.set(s, []);
    schemas.get(s)!.push(entry);
  }

  return (
    <div className="flex flex-col gap-0 rounded-md">
      <ContextMenu>
        <ContextMenuTrigger asChild>
          <div className="group flex items-center gap-0.5 rounded-md text-sidebar-foreground hover:bg-sidebar-accent/50 transition-colors min-h-7">
            <button
              type="button"
              className="shrink-0 p-0.5 rounded hover:bg-sidebar-accent/80 flex items-center justify-center w-5"
              onClick={(e) => {
                e.stopPropagation();
                onToggleExpand();
              }}
            >
              {isExpanded ? (
                <ChevronDown className="size-3.5 text-muted-foreground" />
              ) : (
                <ChevronRight className="size-3.5 text-muted-foreground" />
              )}
            </button>
            <div className="flex-1 flex items-center gap-2 px-1.5 py-1 min-w-0 cursor-pointer" onClick={onToggleExpand}>
              <Icon className={`size-4 shrink-0 ${iconColor}`} />
              <span className="truncate text-xs font-medium flex-1">{connector.name}</span>
              <span className={`shrink-0 text-[9px] px-1.5 py-0.5 rounded-sm font-medium bg-muted ${iconColor}`}>{label}</span>
            </div>
            <Button
              variant="ghost"
              size="icon-xs"
              className="opacity-0 group-hover:opacity-100 shrink-0 h-6 w-6"
              onClick={() => onRemove(connector.id)}
            >
              <Trash2 className="size-3 text-muted-foreground hover:text-destructive" />
            </Button>
          </div>
        </ContextMenuTrigger>
        <ContextMenuContent className="w-48">
          {onRefresh && (
            <ContextMenuItem onSelect={() => onRefresh(connector.id)}>
              <RefreshCw className="size-4 mr-2" />
              Refresh
            </ContextMenuItem>
          )}
          <ContextMenuSeparator />
          <ContextMenuItem variant="destructive" onSelect={() => onRemove(connector.id)}>
            <Trash2 className="size-4 mr-2" />
            Remove
          </ContextMenuItem>
        </ContextMenuContent>
      </ContextMenu>

      {isExpanded && (
        <div className="ml-2 pl-4 border-l border-border/50 flex flex-col py-0.5">
          {Array.from(schemas.entries()).map(([schemaName, tables]) => {
            const schemaKey = `schema-${connector.id}-${schemaName}`;
            const schemaExpanded = expandedIds.has(schemaKey);
            return (
              <div key={schemaName}>
                <button
                  className="flex items-center gap-1.5 py-1 px-1.5 text-[11px] font-medium text-muted-foreground uppercase tracking-wider hover:text-sidebar-foreground w-full text-left"
                  onClick={() => onToggleSchemaExpand(schemaKey)}
                >
                  {schemaExpanded ? (
                    <FolderOpen className="size-3.5 shrink-0" />
                  ) : (
                    <Folder className="size-3.5 shrink-0" />
                  )}
                  <span>{schemaName}</span>
                  <span className="text-[10px] tabular-nums">{tables.length}</span>
                </button>
                {schemaExpanded &&
                  tables.map((entry) => {
                    const existingDs = dataSources.find(
                      (ds) =>
                        ds.connector_id === connector.id &&
                        ds.qualified_name.includes(entry.name)
                    );
                    const tableKey = `table-${connector.id}-${schemaName}-${entry.name}`;
                    const tableExpanded = expandedIds.has(tableKey);
                    const hasColumns = entry.columns && entry.columns.length > 0;
                    return (
                      <div key={entry.name} className="flex flex-col">
                        <ContextMenu>
                          <ContextMenuTrigger asChild>
                            <div
                              className={`flex items-center gap-0.5 py-0.5 pr-2 rounded text-xs min-w-0 w-full text-left hover:bg-sidebar-accent/50 ${
                                existingDs && activeSourceId === existingDs.id
                                  ? "bg-sidebar-accent text-sidebar-accent-foreground"
                                  : "text-sidebar-foreground/90"
                              }`}
                            >
                              <button
                                type="button"
                                className="shrink-0 p-0.5 rounded hover:bg-sidebar-accent/80 flex items-center justify-center w-5 ml-1"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  if (hasColumns) onToggleSchemaExpand(tableKey);
                                }}
                              >
                                {hasColumns ? (
                                  tableExpanded ? (
                                    <ChevronDown className="size-3 text-muted-foreground" />
                                  ) : (
                                    <ChevronRight className="size-3 text-muted-foreground" />
                                  )
                                ) : (
                                  <span className="inline-block w-3" aria-hidden />
                                )}
                              </button>
                              <button
                                className="flex-1 flex items-center gap-2 py-0.5 min-w-0 text-left"
                                onClick={() => {
                                  if (existingDs && onDataSourceSelect) {
                                    onDataSourceSelect(existingDs);
                                  } else if (hasColumns) {
                                    onToggleSchemaExpand(tableKey);
                                  }
                                }}
                              >
                                <Table2 className="size-3 shrink-0 text-muted-foreground/80" />
                                <span className="font-mono truncate text-[11px]">{entry.name}</span>
                                <span className="text-[10px] text-muted-foreground shrink-0">
                                  {entry.entry_type}
                                </span>
                                {hasColumns && (
                                  <span className="shrink-0 text-[10px] text-muted-foreground tabular-nums">
                                    {entry.columns.length}
                                  </span>
                                )}
                              </button>
                            </div>
                          </ContextMenuTrigger>
                          <ContextMenuContent className="w-48">
                            {onNewQuery && connector.alias && (
                              <ContextMenuItem
                                onSelect={() => {
                                  const qn = `"${connector.alias}"."${schemaName}"."${entry.name}"`;
                                  onNewQuery(qn);
                                }}
                              >
                                <TerminalSquare className="size-4 mr-2" />
                                New Query
                              </ContextMenuItem>
                            )}
                            {onImportTable && !existingDs && (
                              <ContextMenuItem
                                onSelect={() =>
                                  onImportTable(connector.id, schemaName, entry.name)
                                }
                              >
                                <Download className="size-4 mr-2" />
                                Import Table
                              </ContextMenuItem>
                            )}
                          </ContextMenuContent>
                        </ContextMenu>
                        {hasColumns && tableExpanded && (
                          <div className="ml-6 pl-3 border-l border-border/50 flex flex-col py-0.5">
                            {entry.columns.map((col) => (
                              <ColumnTreeRow key={col.name} column={col} />
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
              </div>
            );
          })}
          {catalog.length === 0 && (
            <p className="px-3 py-2 text-[11px] text-muted-foreground italic">
              No tables found
            </p>
          )}
        </div>
      )}
    </div>
  );
}

function ColumnTreeRow({ column }: { column: ColumnInfo }) {
  const keyLabel = column.key ? (column.key === "PRI" ? "PK" : column.key) : null;
  return (
    <div
      className="flex items-center gap-2 py-0.5 pl-5 pr-2 rounded text-xs text-sidebar-foreground/90 min-w-0"
      title={`${column.name}: ${column.data_type}${column.nullable ? " (nullable)" : ""}${keyLabel ? ` [${keyLabel}]` : ""}`}
    >
      <LayoutGrid className="size-3 shrink-0 text-muted-foreground/80" />
      <span className="font-mono truncate text-[11px]">{column.name}</span>
      {keyLabel && (
        <span className="shrink-0 text-[9px] px-1 rounded bg-primary/20 text-primary font-medium">
          {keyLabel}
        </span>
      )}
      <span className="text-muted-foreground font-mono shrink-0 text-[10px]">
        {column.data_type}
      </span>
    </div>
  );
}

function GraphTreeItem({
  graph,
  isExpanded,
  onToggleExpand,
  onNewQuery,
  onRunAlgorithm,
  onDrop,
}: {
  graph: PropertyGraphInfo;
  isExpanded: boolean;
  onToggleExpand: () => void;
  onNewQuery?: (graphName: string) => void;
  onRunAlgorithm?: (graphName: string) => void;
  onDrop?: (graphName: string) => void;
}) {
  return (
    <div className="flex flex-col gap-0 rounded-md">
      <ContextMenu>
        <ContextMenuTrigger asChild>
          <div className="group flex items-center gap-0.5 rounded-md text-sidebar-foreground hover:bg-sidebar-accent/50 transition-colors min-h-7">
            <button
              type="button"
              className="shrink-0 p-0.5 rounded hover:bg-sidebar-accent/80 flex items-center justify-center w-5"
              onClick={(e) => {
                e.stopPropagation();
                onToggleExpand();
              }}
            >
              {isExpanded ? (
                <ChevronDown className="size-3.5 text-muted-foreground" />
              ) : (
                <ChevronRight className="size-3.5 text-muted-foreground" />
              )}
            </button>
            <div
              className="flex-1 flex items-center gap-2 px-1.5 py-1 min-w-0 cursor-pointer"
              onClick={onToggleExpand}
            >
              <Network className="size-4 shrink-0 text-cyan-400" />
              <span className="truncate text-xs font-medium flex-1">
                {graph.name}
              </span>
              <span className="shrink-0 text-[9px] px-1.5 py-0.5 rounded-sm font-medium bg-muted text-cyan-400">
                PG
              </span>
            </div>
            {onDrop && (
              <Button
                variant="ghost"
                size="icon-xs"
                className="opacity-0 group-hover:opacity-100 shrink-0 h-6 w-6"
                onClick={() => onDrop(graph.name)}
              >
                <Trash2 className="size-3 text-muted-foreground hover:text-destructive" />
              </Button>
            )}
          </div>
        </ContextMenuTrigger>
        <ContextMenuContent className="w-48">
          {onNewQuery && (
            <ContextMenuItem onSelect={() => onNewQuery(graph.name)}>
              <TerminalSquare className="size-4 mr-2" />
              New Query
            </ContextMenuItem>
          )}
          {onRunAlgorithm && (
            <ContextMenuItem onSelect={() => onRunAlgorithm(graph.name)}>
              <FlaskConical className="size-4 mr-2" />
              Run Algorithm
            </ContextMenuItem>
          )}
          {onDrop && (
            <>
              <ContextMenuSeparator />
              <ContextMenuItem
                variant="destructive"
                onSelect={() => onDrop(graph.name)}
              >
                <Trash2 className="size-4 mr-2" />
                Drop Graph
              </ContextMenuItem>
            </>
          )}
        </ContextMenuContent>
      </ContextMenu>

      {isExpanded && (
        <div className="ml-2 pl-4 border-l border-border/50 flex flex-col py-0.5">
          {graph.vertex_tables.length > 0 && (
            <div className="flex items-center gap-1.5 py-1 px-1.5 text-[11px] font-medium text-muted-foreground uppercase tracking-wider">
              <CircleDot className="size-3 shrink-0" />
              <span>Vertices</span>
            </div>
          )}
          {graph.vertex_tables.map((vt) => (
            <div
              key={vt}
              className="flex items-center gap-2 py-0.5 pl-5 pr-2 rounded text-xs text-sidebar-foreground/90 min-w-0"
            >
              <Table2 className="size-3 shrink-0 text-muted-foreground/80" />
              <span className="font-mono truncate text-[11px]">{vt}</span>
            </div>
          ))}
          {graph.edge_tables.length > 0 && (
            <div className="flex items-center gap-1.5 py-1 px-1.5 text-[11px] font-medium text-muted-foreground uppercase tracking-wider mt-0.5">
              <Link className="size-3 shrink-0" />
              <span>Edges</span>
            </div>
          )}
          {graph.edge_tables.map((et) => (
            <div
              key={et}
              className="flex items-center gap-2 py-0.5 pl-5 pr-2 rounded text-xs text-sidebar-foreground/90 min-w-0"
            >
              <Table2 className="size-3 shrink-0 text-muted-foreground/80" />
              <span className="font-mono truncate text-[11px]">{et}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function CloudConnectorItem({
  connector,
  onRemove,
}: {
  connector: Connector;
  onRemove: (id: string) => void;
}) {
  const Icon = getConnectorIcon(connector);
  const scheme = connector.connector_type === "gcs" ? "gcs" : connector.connector_type === "r2" ? "r2" : "s3";
  const bucket = connector.config.bucket ?? "";
  const prefix = connector.config.prefix ?? "";
  const iconColor = getConnectorIconColor(connector);
  return (
    <div className="group flex items-center gap-0.5 rounded-md text-sidebar-foreground hover:bg-sidebar-accent/50 transition-colors min-h-7">
      <span className="w-5 shrink-0" aria-hidden />
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="flex-1 flex items-center gap-2 px-1.5 py-1 min-w-0">
            <Icon className={`size-4 shrink-0 ${iconColor}`} />
            <span className="truncate text-xs font-medium flex-1">{connector.name}</span>
            <span className={`shrink-0 text-[9px] px-1.5 py-0.5 rounded-sm font-medium bg-muted ${iconColor}`}>
              {connector.connector_type.toUpperCase()}
            </span>
          </div>
        </TooltipTrigger>
        <TooltipContent side="right">
          <p className="text-xs font-mono">
            {scheme}://{bucket}
            {prefix ? `/${prefix}` : ""}
          </p>
          <p className="text-xs text-muted-foreground mt-0.5">
            {connector.connector_type.toUpperCase()} · {connector.config.region ?? ""}
          </p>
        </TooltipContent>
      </Tooltip>
      <Button
        variant="ghost"
        size="icon-xs"
        className="opacity-0 group-hover:opacity-100 shrink-0 h-6 w-6"
        onClick={() => onRemove(connector.id)}
      >
        <Trash2 className="size-3 text-muted-foreground hover:text-destructive" />
      </Button>
    </div>
  );
}
