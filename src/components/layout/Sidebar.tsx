import { useState, useCallback } from "react";
import type { LucideIcon } from "lucide-react";
import {
  FileSpreadsheet,
  FileText,
  FileJson,
  Cloud,
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
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
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
import type { DataSource, ConnectionInfo, ColumnInfo } from "@/lib/types";

function getDataSourceIcon(source: DataSource): LucideIcon {
  if (source.source_type === "s3" || source.connection_id) return Cloud;
  const fmt = (source.format ?? "").toLowerCase();
  switch (fmt) {
    case "csv":
    case "tsv":
      return FileText;
    case "json":
    case "jsonl":
      return FileJson;
    case "parquet":
      return Database;
    case "xlsx":
      return FileSpreadsheet;
    case "arrow_ipc":
    case "arrow":
      return Table2;
    default:
      return FileSpreadsheet;
  }
}
import type { QueryHistoryEntry } from "@/hooks/useQuery";
import type { ViewMode } from "@/hooks/useWorkspaceTabs";

interface SidebarProps {
  dataSources: DataSource[];
  connections: ConnectionInfo[];
  activeSourceId: string | null;
  queryHistory: QueryHistoryEntry[];
  onAddDataSource: () => void;
  onDataSourceSelect: (ds: DataSource, viewMode?: ViewMode) => void;
  onDataSourceRemove: (id: string) => void;
  onAddConnection: () => void;
  onConnectionRemove: (id: string) => void;
  onQuerySelect: (sql: string) => void;
  onNewQuery?: (ds: DataSource) => void;
  onOpenDataTab?: (ds: DataSource, viewMode?: ViewMode) => void;
  onViewDataAsQuery?: (ds: DataSource, viewMode: ViewMode) => void;
  onExport?: (ds: DataSource) => void;
  onImport?: (ds: DataSource) => void;
  onProperties?: (ds: DataSource) => void;
  onOpenSettings?: () => void;
}

export function Sidebar({
  dataSources,
  connections,
  activeSourceId,
  queryHistory,
  onAddDataSource,
  onDataSourceSelect,
  onDataSourceRemove,
  onAddConnection,
  onConnectionRemove,
  onQuerySelect,
  onNewQuery,
  onOpenDataTab,
  onViewDataAsQuery,
  onExport,
  onImport,
  onProperties,
  onOpenSettings,
}: SidebarProps) {
  const [expandedSourceIds, setExpandedSourceIds] = useState<Set<string>>(new Set());
  const toggleExpanded = useCallback((id: string) => {
    setExpandedSourceIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  return (
    <aside className="w-full bg-sidebar flex flex-col h-full min-w-0">
      <div className="flex items-center gap-2 h-10 px-4 border-b border-border shrink-0">
        <Database className="size-4 text-primary shrink-0" />
        <h1 className="text-sm font-semibold text-sidebar-foreground tracking-tight truncate">
          Columnar Data Viewer
        </h1>
      </div>

      <ScrollArea className="flex-1">
        <div className="py-1.5 px-1">
          {/* Data Sources */}
          <div className="mb-2">
            <div className="flex items-center justify-between gap-1 px-2 py-1 shrink-0">
              <span className="text-[11px] font-medium text-muted-foreground uppercase tracking-wider truncate min-w-0">
                Data Sources
              </span>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button variant="ghost" size="icon-xs" onClick={onAddDataSource} className="shrink-0 text-muted-foreground hover:text-sidebar-foreground h-6 w-6">
                    <Plus className="size-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="right">Add data source</TooltipContent>
              </Tooltip>
            </div>

            {dataSources.length === 0 ? (
              <p className="px-3 py-4 text-xs text-muted-foreground text-center italic">
                No data sources
              </p>
            ) : (
              <div className="space-y-0.5">
                {dataSources.map((ds) => (
                  <DataSourceTreeItem
                    key={ds.id}
                    source={ds}
                    isActive={activeSourceId === ds.id}
                    isExpanded={expandedSourceIds.has(ds.id)}
                    onToggleExpand={() => toggleExpanded(ds.id)}
                    onSelect={onDataSourceSelect}
                    onRemove={onDataSourceRemove}
                    onNewQuery={onNewQuery}
                    onOpenDataTab={onOpenDataTab}
                    onViewDataAsQuery={onViewDataAsQuery}
                    onExport={onExport}
                    onImport={onImport}
                    onProperties={onProperties}
                  />
                ))}
              </div>
            )}
          </div>

          <Separator className="my-2" />

          {/* Connections */}
          <div className="mb-2">
            <div className="flex items-center justify-between px-2 py-1">
              <span className="text-[11px] font-medium text-muted-foreground uppercase tracking-wider">
                Connections
              </span>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button variant="ghost" size="icon-xs" onClick={onAddConnection} className="text-muted-foreground hover:text-sidebar-foreground">
                    <Plus className="size-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="right">Add connection</TooltipContent>
              </Tooltip>
            </div>

            {connections.length === 0 ? (
              <p className="px-3 py-4 text-xs text-muted-foreground text-center italic">
                No connections
              </p>
            ) : (
              <div className="space-y-0.5">
                {connections.map((conn) => (
                  <ConnectionItem
                    key={conn.id}
                    connection={conn}
                    onRemove={onConnectionRemove}
                  />
                ))}
              </div>
            )}
          </div>

          <Separator className="my-2" />

          {/* Query History */}
          <div>
            <div className="flex items-center gap-1.5 px-2 py-1">
              <span className="text-[11px] font-medium text-muted-foreground uppercase tracking-wider">
                Query History
              </span>
            </div>

            {queryHistory.length === 0 ? (
              <p className="px-3 py-4 text-xs text-muted-foreground text-center italic">
                No queries yet
              </p>
            ) : (
              <div className="space-y-0.5 mt-0.5">
                {queryHistory.slice(0, 10).map((entry, i) => (
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
        </div>
      </ScrollArea>
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
  isActive,
  isExpanded,
  onToggleExpand,
  onSelect,
  onRemove,
  onNewQuery,
  onOpenDataTab,
  onViewDataAsQuery,
  onExport,
  onImport,
  onProperties,
}: {
  source: DataSource;
  isActive: boolean;
  isExpanded: boolean;
  onToggleExpand: () => void;
  onSelect: (ds: DataSource, viewMode?: ViewMode) => void;
  onRemove: (id: string) => void;
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
                const Icon = getDataSourceIcon(source);
                return <Icon className="size-4 shrink-0 text-muted-foreground" />;
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
            {isExpanded ? (
              <FolderOpen className="size-3.5 shrink-0" />
            ) : (
              <Folder className="size-3.5 shrink-0" />
            )}
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

function ConnectionItem({
  connection,
  onRemove,
}: {
  connection: ConnectionInfo;
  onRemove: (id: string) => void;
}) {
  const prefix = connection.provider === "gcp" ? "gcs" : connection.provider === "cloudflare" ? "r2" : "s3";
  return (
    <div className="group flex items-center gap-0.5 rounded-md text-sidebar-foreground hover:bg-sidebar-accent/50 transition-colors min-h-7">
      <span className="w-5 shrink-0" aria-hidden />
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="flex-1 flex items-center gap-2 px-1.5 py-1 min-w-0">
            <Database className="size-4 shrink-0 text-muted-foreground" />
            <span className="truncate text-xs font-medium">{connection.name}</span>
          </div>
        </TooltipTrigger>
        <TooltipContent side="right">
          <p className="text-xs font-mono">
            {prefix}://{connection.bucket}
            {connection.prefix ? `/${connection.prefix}` : ""}
          </p>
          <p className="text-xs text-muted-foreground mt-0.5">
            {connection.provider.toUpperCase()} · {connection.region}
          </p>
        </TooltipContent>
      </Tooltip>
      <Button
        variant="ghost"
        size="icon-xs"
        className="opacity-0 group-hover:opacity-100 shrink-0 h-6 w-6"
        onClick={() => onRemove(connection.id)}
      >
        <Trash2 className="size-3 text-muted-foreground hover:text-destructive" />
      </Button>
    </div>
  );
}
