import { useState, useCallback } from "react";
import type { QueryResult } from "../lib/types";
import { runQuery, runPaginatedQuery } from "../lib/ipc";
import { decodeArrowIPC } from "../lib/arrow";

export interface QueryHistoryEntry {
  sql: string;
  timestamp: number;
  status: "success" | "error";
  executionTimeMs: number;
  error?: string;
}

export function useQueryEngine() {
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [executionTimeMs, setExecutionTimeMs] = useState<number | null>(null);
  const [history, setHistory] = useState<QueryHistoryEntry[]>([]);
  const [page, setPage] = useState(0);
  const [pageSize] = useState(1000);

  const executeQuery = useCallback(async (sql: string) => {
    setLoading(true);
    setError(null);
    setResult(null);
    setPage(0);
    const start = performance.now();

    try {
      const ipcData = await runQuery(sql);
      const elapsed = Math.round(performance.now() - start);
      setExecutionTimeMs(elapsed);
      const decoded = decodeArrowIPC(ipcData);
      setResult(decoded);
      setHistory((prev) => [
        { sql, timestamp: Date.now(), status: "success", executionTimeMs: elapsed },
        ...prev.slice(0, 19),
      ]);
    } catch (e) {
      const elapsed = Math.round(performance.now() - start);
      setExecutionTimeMs(elapsed);
      const msg = typeof e === "string" ? e : String(e);
      setError(msg);
      setHistory((prev) => [
        { sql, timestamp: Date.now(), status: "error", executionTimeMs: elapsed, error: msg },
        ...prev.slice(0, 19),
      ]);
    } finally {
      setLoading(false);
    }
  }, []);

  const paginateQuery = useCallback(
    async (sql: string, newPage: number) => {
      setLoading(true);
      setError(null);
      try {
        const ipcData = await runPaginatedQuery(sql, newPage, pageSize);
        const decoded = decodeArrowIPC(ipcData);
        setResult(decoded);
        setPage(newPage);
      } catch (e) {
        setError(typeof e === "string" ? e : String(e));
      } finally {
        setLoading(false);
      }
    },
    [pageSize]
  );

  return {
    result,
    loading,
    error,
    executionTimeMs,
    history,
    page,
    pageSize,
    executeQuery,
    paginateQuery,
  };
}
