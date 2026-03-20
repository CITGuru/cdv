import { memo } from "react";
import {
  Trash2,
  Table2,
  TerminalSquare,
  Download,
  Upload,
  Info,
  ChevronRight,
  ChevronDown,
  FolderOpen,
  RefreshCw,
} from "lucide-react";
import { Button } from "@/components/ui/button";
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
import type { DataSource, Connector } from "@/lib/types";
import type { ViewMode } from "@/hooks/useWorkspaceTabs";
import { getDataSourceIcon, getFileIconColor } from "./sidebar-icons";
import { ColumnTreeRow } from "./ColumnTreeRow";

export const DataSourceTreeItem = memo(function DataSourceTreeItem({
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
  const Icon = getDataSourceIcon(source, connector);
  const iconColor = getFileIconColor(connector);

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
              <Icon className={`size-4 shrink-0 ${iconColor}`} />
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
});
