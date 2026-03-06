import type { QueryResult } from "../../lib/types";
import { DataTable } from "../dataset/DataTable";

interface ResultsTableProps {
  result: QueryResult | null;
  loading: boolean;
  error: string | null;
  executionTimeMs: number | null;
}

export function ResultsTable({
  result,
  loading,
  error,
  executionTimeMs,
}: ResultsTableProps) {
  if (error) {
    return (
      <div className="flex flex-col h-full">
        <div className="px-4 py-2 border-b border-zinc-700 bg-zinc-900 text-xs text-zinc-400">
          Query failed
          {executionTimeMs !== null && (
            <span className="ml-2 text-zinc-600">({executionTimeMs}ms)</span>
          )}
        </div>
        <div className="p-4">
          <div className="px-3 py-2 bg-red-900/30 border border-red-800 rounded text-sm text-red-300 font-mono whitespace-pre-wrap">
            {error}
          </div>
        </div>
      </div>
    );
  }

  if (!result && !loading) {
    return (
      <div className="flex items-center justify-center h-full text-zinc-500 text-sm">
        Run a query to see results
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* Status bar */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-zinc-700 bg-zinc-900 text-xs text-zinc-400 shrink-0">
        <div className="flex items-center gap-4">
          {result && (
            <>
              <span>{result.columns.length} columns</span>
              <span>{result.rows.length.toLocaleString()} rows</span>
            </>
          )}
        </div>
        {executionTimeMs !== null && (
          <span className="text-zinc-500">{executionTimeMs}ms</span>
        )}
      </div>

      <div className="flex-1 overflow-hidden relative">
        {loading && (
          <div className="absolute inset-0 bg-zinc-950/50 flex items-center justify-center z-20">
            <div className="text-sm text-zinc-400">Executing query...</div>
          </div>
        )}
        {result && (
          <DataTable columns={result.columns} rows={result.rows} />
        )}
      </div>
    </div>
  );
}
