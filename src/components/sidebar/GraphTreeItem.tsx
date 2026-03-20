import {
  CircleDot,
  Trash2,
  Table2,
  TerminalSquare,
  ChevronRight,
  ChevronDown,
  Link,
  Network,
  FlaskConical,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from "@/components/ui/context-menu";
import type { PropertyGraphInfo } from "@/lib/types";

export function GraphTreeItem({
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
