import { useState, useEffect, useRef } from "react";
import {
  Loader2,
  CheckCircle2,
  XCircle,
  Circle,
  MinusCircle,
  Square,
  ChevronDown,
  ChevronRight,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import type {
  EtlJob,
  Connector,
  EtlProgressEvent,
  EtlCompleteEvent,
} from "@/lib/types";

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  const remSecs = secs % 60;
  if (mins < 60) return `${mins}m ${remSecs}s`;
  const hours = Math.floor(mins / 60);
  const remMins = mins % 60;
  return `${hours}h ${remMins}m`;
}

interface EtlProgressProps {
  open: boolean;
  onClose: () => void;
  job: EtlJob | null;
  connectors: Connector[];
  activeProgress: EtlProgressEvent | null;
  lastComplete: EtlCompleteEvent | null;
  onCancel: () => void;
}

export function EtlProgress({
  open,
  onClose,
  job,
  connectors,
  activeProgress,
  lastComplete,
  onCancel,
}: EtlProgressProps) {
  const [elapsedMs, setElapsedMs] = useState(0);
  const [expandedErrors, setExpandedErrors] = useState<Set<number>>(new Set());
  const startRef = useRef<number>(Date.now());
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open || !job) return;
    startRef.current = Date.now();
    setElapsedMs(0);
    setExpandedErrors(new Set());
  }, [open, job?.id]);

  useEffect(() => {
    if (!open || job?.status !== "running") return;
    const interval = setInterval(() => {
      setElapsedMs(Date.now() - startRef.current);
    }, 1000);
    return () => clearInterval(interval);
  }, [open, job?.status]);

  if (!job) return null;

  const isRunning = job.status === "running";
  const isDone = lastComplete?.job_id === job.id;
  const sourceName = connectors.find((c) => c.id === job.source_connector_id)?.name ?? "Source";
  const targetName = connectors.find((c) => c.id === job.target_connector_id)?.name ?? "Target";

  const currentIndex = activeProgress?.current_table_index ?? 0;
  const totalTables = activeProgress?.total_tables ?? job.table_states.length;
  const completedCount = job.table_states.filter((t) => t.status === "completed").length;
  const failedCount = job.table_states.filter((t) => t.status === "failed").length;
  const progressPct = totalTables > 0
    ? Math.round(((isDone ? totalTables : currentIndex) / totalTables) * 100)
    : 0;

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="w-[95vw] max-w-xl sm:max-w-xl min-h-[400px] max-h-[80vh] flex flex-col gap-0 p-0">
        <div className="px-6 pt-5 pb-3">
          <DialogTitle className="text-base font-semibold">
            {isRunning ? "Migration in Progress" : "Migration Result"}
          </DialogTitle>
          <p className="text-xs text-muted-foreground mt-1">
            {sourceName} &rarr; {targetName}
          </p>
        </div>

        {/* Progress bar */}
        <div className="px-6 pb-3">
          <div className="flex items-center justify-between text-xs text-muted-foreground mb-1.5">
            <span>
              {isDone
                ? `${completedCount} migrated, ${failedCount} failed`
                : isRunning
                  ? `${currentIndex + 1} of ${totalTables} tables`
                  : `${completedCount}/${totalTables} tables`}
            </span>
            <span>{formatDuration(isDone ? (lastComplete?.elapsed_ms ?? elapsedMs) : elapsedMs)}</span>
          </div>
          <div className="h-2 bg-muted rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full transition-all duration-500 ${
                failedCount > 0 && isDone
                  ? "bg-yellow-500"
                  : isDone
                    ? "bg-green-500"
                    : "bg-primary"
              }`}
              style={{ width: `${progressPct}%` }}
            />
          </div>
        </div>

        <Separator />

        {/* Table list */}
        <ScrollArea className="flex-1 min-h-0 px-6 py-3" ref={scrollRef}>
          <div className="space-y-0.5">
            {job.table_states.map((ts, idx) => {
              const isActive = isRunning && activeProgress?.current_table_index === idx && activeProgress?.status === "running";
              const errorExpanded = expandedErrors.has(idx);

              return (
                <div key={`${ts.schema_name}.${ts.table_name}`}>
                  <div
                    className={`flex items-center gap-2 px-2 py-1 rounded text-xs ${
                      isActive ? "bg-primary/5" : ""
                    }`}
                  >
                    <TableStatusIcon status={ts.status} isActive={isActive} />
                    <span className="font-mono truncate flex-1 min-w-0">
                      {ts.schema_name}.{ts.table_name}
                    </span>
                    {ts.rows_synced != null && (
                      <span className="text-muted-foreground shrink-0">
                        {ts.rows_synced.toLocaleString()} rows
                      </span>
                    )}
                    {ts.error && (
                      <button
                        onClick={() => {
                          const next = new Set(expandedErrors);
                          if (errorExpanded) next.delete(idx);
                          else next.add(idx);
                          setExpandedErrors(next);
                        }}
                        className="shrink-0"
                      >
                        {errorExpanded ? (
                          <ChevronDown className="size-3 text-destructive" />
                        ) : (
                          <ChevronRight className="size-3 text-destructive" />
                        )}
                      </button>
                    )}
                  </div>
                  {ts.error && errorExpanded && (
                    <div className="ml-6 px-2 py-1 text-[10px] text-destructive font-mono bg-destructive/5 rounded mb-1">
                      {ts.error}
                    </div>
                  )}
                </div>
              );
            })}

            {isRunning && job.table_states.length === 0 && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-4 justify-center">
                <Loader2 className="size-4 animate-spin" />
                Preparing migration...
              </div>
            )}
          </div>
        </ScrollArea>

        <Separator />

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-3">
          <div>
            {isDone && lastComplete && (
              <div className="flex items-center gap-2">
                {lastComplete.status === "completed" ? (
                  <Badge variant="secondary" className="text-green-600">
                    <CheckCircle2 className="size-3 mr-1" />
                    Completed
                  </Badge>
                ) : lastComplete.status === "partial" ? (
                  <Badge variant="secondary" className="text-yellow-600">
                    Partial
                  </Badge>
                ) : lastComplete.status === "cancelled" ? (
                  <Badge variant="outline">Cancelled</Badge>
                ) : (
                  <Badge variant="destructive">Failed</Badge>
                )}
                {lastComplete.total_rows > 0 && (
                  <span className="text-xs text-muted-foreground">
                    {lastComplete.total_rows.toLocaleString()} total rows
                  </span>
                )}
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            {isRunning && (
              <Button variant="destructive" size="sm" onClick={onCancel}>
                <Square className="size-3 mr-1" />
                Cancel
              </Button>
            )}
            <Button variant="outline" size="sm" onClick={onClose}>
              {isDone || !isRunning ? "Close" : "Hide"}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function TableStatusIcon({ status, isActive }: { status: string; isActive: boolean }) {
  if (isActive) return <Loader2 className="size-3.5 animate-spin text-blue-500 shrink-0" />;
  switch (status) {
    case "completed":
      return <CheckCircle2 className="size-3.5 text-green-500 shrink-0" />;
    case "failed":
      return <XCircle className="size-3.5 text-destructive shrink-0" />;
    case "skipped":
      return <MinusCircle className="size-3.5 text-muted-foreground shrink-0" />;
    case "running":
      return <Loader2 className="size-3.5 animate-spin text-blue-500 shrink-0" />;
    default:
      return <Circle className="size-3.5 text-muted-foreground/40 shrink-0" />;
  }
}
