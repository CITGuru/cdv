import { memo } from "react";
import {
  Play,
  Square,
  Trash2,
  Loader2,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Clock,
  ArrowRight,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from "@/components/ui/context-menu";
import type { EtlJob, Connector, EtlProgressEvent } from "@/lib/types";

function formatRelativeTime(iso: string | null): string {
  if (!iso) return "";
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function StatusIcon({ status }: { status: EtlJob["status"] }) {
  switch (status) {
    case "running":
      return <Loader2 className="size-3.5 animate-spin text-blue-500" />;
    case "completed":
      return <CheckCircle2 className="size-3.5 text-green-500" />;
    case "failed":
      return <XCircle className="size-3.5 text-destructive" />;
    case "cancelled":
      return <Square className="size-3.5 text-muted-foreground" />;
    case "partial":
      return <AlertTriangle className="size-3.5 text-yellow-500" />;
    default:
      return <Clock className="size-3.5 text-muted-foreground" />;
  }
}

function statusLabel(status: EtlJob["status"]): string {
  switch (status) {
    case "idle": return "Idle";
    case "running": return "Running";
    case "completed": return "Completed";
    case "failed": return "Failed";
    case "cancelled": return "Cancelled";
    case "partial": return "Partial";
    default: return status;
  }
}

function statusVariant(status: EtlJob["status"]): "default" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "running": return "default";
    case "completed": return "secondary";
    case "failed": return "destructive";
    default: return "outline";
  }
}

interface EtlJobListProps {
  jobs: EtlJob[];
  connectors: Connector[];
  activeProgress: EtlProgressEvent | null;
  onRunJob: (jobId: string) => void;
  onCancelJob: () => void;
  onDeleteJob: (jobId: string) => void;
  onViewProgress: (jobId: string) => void;
}

export const EtlJobList = memo(function EtlJobList({
  jobs,
  connectors,
  activeProgress,
  onRunJob,
  onCancelJob,
  onDeleteJob,
  onViewProgress,
}: EtlJobListProps) {
  if (jobs.length === 0) return null;

  const getConnectorName = (id: string) =>
    connectors.find((c) => c.id === id)?.name ?? "Unknown";

  return (
    <div className="space-y-0.5">
      {jobs.map((job) => {
        const isRunning = job.status === "running";
        const progress = isRunning && activeProgress?.job_id === job.id
          ? activeProgress
          : null;

        return (
          <ContextMenu key={job.id}>
            <ContextMenuTrigger>
              <button
                onClick={() => isRunning ? onViewProgress(job.id) : undefined}
                className="w-full flex flex-col gap-0.5 px-2 py-1.5 rounded-md text-left hover:bg-sidebar-accent transition-colors min-h-7"
              >
                <div className="flex items-center gap-1.5 min-w-0">
                  <StatusIcon status={job.status} />
                  <span className="text-xs truncate flex-1 min-w-0">
                    {getConnectorName(job.source_connector_id)}
                  </span>
                  <ArrowRight className="size-3 text-muted-foreground shrink-0" />
                  <span className="text-xs truncate flex-1 min-w-0">
                    {getConnectorName(job.target_connector_id)}
                  </span>
                </div>
                <div className="flex items-center gap-1.5 pl-5">
                  <Badge variant={statusVariant(job.status)} className="text-[9px] px-1 py-0 h-4">
                    {progress
                      ? `${progress.current_table_index + 1}/${progress.total_tables}`
                      : statusLabel(job.status)}
                  </Badge>
                  {job.last_run_at && (
                    <span className="text-[10px] text-muted-foreground truncate">
                      {formatRelativeTime(job.last_run_at)}
                    </span>
                  )}
                </div>
              </button>
            </ContextMenuTrigger>
            <ContextMenuContent className="min-w-40">
              {isRunning ? (
                <>
                  <ContextMenuItem onSelect={() => onViewProgress(job.id)}>
                    <Loader2 className="size-4 mr-2" />
                    View Progress
                  </ContextMenuItem>
                  <ContextMenuItem onSelect={onCancelJob}>
                    <Square className="size-4 mr-2" />
                    Cancel
                  </ContextMenuItem>
                </>
              ) : (
                <ContextMenuItem onSelect={() => onRunJob(job.id)}>
                  <Play className="size-4 mr-2" />
                  Run Now
                </ContextMenuItem>
              )}
              <ContextMenuSeparator />
              <ContextMenuItem
                onSelect={() => onDeleteJob(job.id)}
                disabled={isRunning}
                className="text-destructive focus:text-destructive"
              >
                <Trash2 className="size-4 mr-2" />
                Delete
              </ContextMenuItem>
            </ContextMenuContent>
          </ContextMenu>
        );
      })}
    </div>
  );
});
