import {
  ChevronFirst,
  ChevronLast,
  ChevronLeft,
  ChevronRight,
  Loader2,
  Rows3,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { DataSource, QueryResult, PaginationState } from "@/lib/types";
import type { ParsedError } from "@/lib/errors";
import { SchemaPanel } from "./SchemaPanel";
import { DataTable } from "./DataTable";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";

interface DatasetViewerProps {
  dataset: DataSource;
  previewData: QueryResult | null;
  loading: boolean;
  error: ParsedError | null;
  pagination: PaginationState;
  onPageChange: (page: number) => void;
  onPageSizeChange: (pageSize: number) => void;
}

export function DatasetViewer({
  dataset,
  previewData,
  loading,
  error,
  pagination,
  onPageChange,
  onPageSizeChange,
}: DatasetViewerProps) {
  const totalPages = pagination.totalRows
    ? Math.ceil(pagination.totalRows / pagination.pageSize)
    : null;

  return (
    <div className="flex flex-col h-full">
      <SchemaPanel dataset={dataset} />

      {error && (
        <div className="mx-3 mt-2">
          <ErrorDisplay error={error} compact />
        </div>
      )}

      <div className="flex-1 overflow-hidden relative">
        {loading && (
          <div className="absolute inset-0 bg-background/60 backdrop-blur-sm flex items-center justify-center z-20">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" />
              Loading data...
            </div>
          </div>
        )}
        {previewData && (
          <DataTable columns={previewData.columns} rows={previewData.rows} />
        )}
      </div>

      {/* Pagination Bar */}
      <div className="flex items-center justify-between px-3 py-1.5 border-t border-border bg-card text-xs text-muted-foreground shrink-0">
        <div className="flex items-center gap-2">
          <span>Rows per page</span>
          <Select
            value={String(pagination.pageSize)}
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
          <span className="tabular-nums">
            Page {pagination.page + 1}
            {totalPages ? ` of ${totalPages.toLocaleString()}` : ""}
          </span>
          <div className="flex gap-0.5">
            <Button
              variant="ghost"
              size="icon-xs"
              onClick={() => onPageChange(0)}
              disabled={pagination.page === 0}
            >
              <ChevronFirst className="size-3.5" />
            </Button>
            <Button
              variant="ghost"
              size="icon-xs"
              onClick={() => onPageChange(pagination.page - 1)}
              disabled={pagination.page === 0}
            >
              <ChevronLeft className="size-3.5" />
            </Button>
            <Button
              variant="ghost"
              size="icon-xs"
              onClick={() => onPageChange(pagination.page + 1)}
              disabled={totalPages !== null && pagination.page + 1 >= totalPages}
            >
              <ChevronRight className="size-3.5" />
            </Button>
            <Button
              variant="ghost"
              size="icon-xs"
              onClick={() => onPageChange((totalPages ?? 1) - 1)}
              disabled={totalPages !== null && pagination.page + 1 >= totalPages}
            >
              <ChevronLast className="size-3.5" />
            </Button>
          </div>
        </div>

        <div className="flex items-center gap-1">
          <Rows3 className="size-3" />
          <span className="tabular-nums">
            {pagination.totalRows?.toLocaleString() ?? "?"} total rows
          </span>
        </div>
      </div>
    </div>
  );
}
