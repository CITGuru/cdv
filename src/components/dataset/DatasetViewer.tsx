import type { DatasetInfo, QueryResult, PaginationState } from "../../lib/types";
import { SchemaPanel } from "./SchemaPanel";
import { DataTable } from "./DataTable";

interface DatasetViewerProps {
  dataset: DatasetInfo;
  previewData: QueryResult | null;
  loading: boolean;
  error: string | null;
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
        <div className="mx-4 mt-2 px-3 py-2 bg-red-900/30 border border-red-800 rounded text-sm text-red-300">
          {error}
        </div>
      )}

      <div className="flex-1 overflow-hidden relative">
        {loading && (
          <div className="absolute inset-0 bg-zinc-950/50 flex items-center justify-center z-20">
            <div className="text-sm text-zinc-400">Loading...</div>
          </div>
        )}
        {previewData && (
          <DataTable columns={previewData.columns} rows={previewData.rows} />
        )}
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between px-4 py-2 border-t border-zinc-700 bg-zinc-900 text-xs text-zinc-400 shrink-0">
        <div className="flex items-center gap-2">
          <span>Rows per page:</span>
          <select
            value={pagination.pageSize}
            onChange={(e) => onPageSizeChange(Number(e.target.value))}
            className="bg-zinc-800 border border-zinc-700 text-zinc-300 rounded px-2 py-1 text-xs"
          >
            <option value={100}>100</option>
            <option value={500}>500</option>
            <option value={1000}>1,000</option>
          </select>
        </div>

        <div className="flex items-center gap-3">
          <span>
            Page {pagination.page + 1}
            {totalPages ? ` of ${totalPages.toLocaleString()}` : ""}
          </span>
          <div className="flex gap-1">
            <button
              onClick={() => onPageChange(0)}
              disabled={pagination.page === 0}
              className="px-2 py-1 bg-zinc-800 rounded hover:bg-zinc-700 disabled:opacity-30 disabled:cursor-not-allowed"
            >
              ««
            </button>
            <button
              onClick={() => onPageChange(pagination.page - 1)}
              disabled={pagination.page === 0}
              className="px-2 py-1 bg-zinc-800 rounded hover:bg-zinc-700 disabled:opacity-30 disabled:cursor-not-allowed"
            >
              «
            </button>
            <button
              onClick={() => onPageChange(pagination.page + 1)}
              disabled={totalPages !== null && pagination.page + 1 >= totalPages}
              className="px-2 py-1 bg-zinc-800 rounded hover:bg-zinc-700 disabled:opacity-30 disabled:cursor-not-allowed"
            >
              »
            </button>
            <button
              onClick={() => onPageChange((totalPages ?? 1) - 1)}
              disabled={totalPages !== null && pagination.page + 1 >= totalPages}
              className="px-2 py-1 bg-zinc-800 rounded hover:bg-zinc-700 disabled:opacity-30 disabled:cursor-not-allowed"
            >
              »»
            </button>
          </div>
        </div>

        <div>
          {pagination.totalRows?.toLocaleString() ?? "?"} total rows
        </div>
      </div>
    </div>
  );
}
