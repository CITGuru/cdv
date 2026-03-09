import {
  Loader2,
  Rows3,
  Clock,
  TerminalSquare,
  ChevronFirst,
  ChevronLast,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { QueryResult } from "@/lib/types";
import type { ParsedError } from "@/lib/errors";
import { DataTable } from "@/components/dataset/DataTable";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";

interface ResultsTableProps {
  result: QueryResult | null;
  loading: boolean;
  error: ParsedError | null;
  executionTimeMs: number | null;
  page: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (pageSize: number) => void;
}

export function ResultsTable({
  result,
  loading,
  error,
  executionTimeMs,
  page,
  pageSize,
  onPageChange,
  onPageSizeChange,
}: ResultsTableProps) {
  if (error) {
    return (
      <div className="flex flex-col h-full">
        <div className="flex items-center gap-2 px-3 py-1.5 border-b border-border bg-card text-xs text-muted-foreground">
          <span className="text-destructive font-medium">Query failed</span>
          {executionTimeMs !== null && (
            <Badge
              variant="outline"
              className="text-[10px] px-1.5 py-0 h-4 gap-1"
            >
              <Clock className="size-2.5" />
              {executionTimeMs}ms
            </Badge>
          )}
        </div>
        <div className="p-4 overflow-auto flex-1">
          <ErrorDisplay error={error} />
        </div>
      </div>
    );
  }

  if (!result && !loading) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-2 text-muted-foreground">
        <TerminalSquare className="size-8 opacity-30" />
        <span className="text-sm">Run a query to see results</span>
      </div>
    );
  }

  const hasFullPage = result != null && result.rows.length >= pageSize;

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-hidden relative">
        {loading && (
          <div className="absolute inset-0 bg-background/60 backdrop-blur-sm flex items-center justify-center z-20">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" />
              Executing query...
            </div>
          </div>
        )}
        {result && (
          <DataTable columns={result.columns} rows={result.rows} />
        )}
      </div>

      {result && (
        <div className="flex items-center justify-between px-3 py-1.5 border-t border-border bg-card text-xs text-muted-foreground shrink-0">
          <div className="flex items-center gap-2">
            <span>Rows per page</span>
            <Select
              value={String(pageSize)}
              onValueChange={(v) => onPageSizeChange(Number(v))}
            >
              <SelectTrigger className="h-6 w-20 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="100">100</SelectItem>
                <SelectItem value="500">500</SelectItem>
                <SelectItem value="1000">1,000</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="flex items-center gap-1.5">
            {executionTimeMs !== null && (
              <Badge
                variant="outline"
                className="text-[10px] px-1.5 py-0 h-4 gap-1 mr-2"
              >
                <Clock className="size-2.5" />
                {executionTimeMs}ms
              </Badge>
            )}
            <span className="tabular-nums">Page {page + 1}</span>
            <div className="flex gap-0.5">
              <Button
                variant="ghost"
                size="icon-xs"
                onClick={() => onPageChange(0)}
                disabled={page === 0 || loading}
              >
                <ChevronFirst className="size-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon-xs"
                onClick={() => onPageChange(page - 1)}
                disabled={page === 0 || loading}
              >
                <ChevronLeft className="size-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon-xs"
                onClick={() => onPageChange(page + 1)}
                disabled={!hasFullPage || loading}
              >
                <ChevronRight className="size-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon-xs"
                onClick={() => onPageChange(page + 1)}
                disabled={!hasFullPage || loading}
              >
                <ChevronLast className="size-3.5" />
              </Button>
            </div>
          </div>

          <div className="flex items-center gap-1">
            <Rows3 className="size-3" />
            <span className="tabular-nums">
              {result.rows.length.toLocaleString()} rows
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
