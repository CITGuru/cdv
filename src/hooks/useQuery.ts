import { useState, useCallback, useMemo, useRef } from "react";
import { listen } from "@tauri-apps/api/event";
import type { QueryResult } from "../lib/types";
import type { ParsedError } from "../lib/errors";
import { extractError } from "../lib/errors";
import { runPaginatedQuery, streamQuery } from "../lib/ipc";
import { decodeArrowIPC } from "../lib/arrow";

export interface QueryHistoryEntry {
  sql: string;
  timestamp: number;
  status: "success" | "error";
  executionTimeMs: number;
  error?: string;
}

interface TabQueryState {
  result: QueryResult | null;
  error: ParsedError | null;
  executionTimeMs: number | null;
  lastSql: string | null;
  page: number;
}

const DEFAULT_PAGE_SIZE = 100;

const EMPTY_TAB_STATE: TabQueryState = {
  result: null,
  error: null,
  executionTimeMs: null,
  lastSql: null,
  page: 0,
};

export function useQueryEngine() {
  const [tabStates, setTabStates] = useState<Record<string, TabQueryState>>({});
  const [loading, setLoading] = useState(false);
  const [activeTabId, setActiveQueryTabId] = useState<string | null>(null);
  const [history, setHistory] = useState<QueryHistoryEntry[]>([]);
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);

  const activeTabIdRef = useRef(activeTabId);
  activeTabIdRef.current = activeTabId;
  const runningRef = useRef(false);

  const updateTabState = useCallback((tabId: string, patch: Partial<TabQueryState>) => {
    setTabStates((prev) => ({
      ...prev,
      [tabId]: { ...(prev[tabId] ?? EMPTY_TAB_STATE), ...patch },
    }));
  }, []);

  const executeQuery = useCallback(
    async (sql: string, options?: { useStreaming?: boolean; tabId?: string }) => {
      if (runningRef.current) return;
      runningRef.current = true;
      const tabId = options?.tabId ?? activeTabIdRef.current;
      setLoading(true);
      if (tabId) updateTabState(tabId, { error: null, result: null, lastSql: sql, page: 0 });
      const start = performance.now();

      try {
        if (options?.useStreaming) {
          const chunks: number[][] = [];
          let resolveComplete: () => void;
          const completePromise = new Promise<void>((r) => {
            resolveComplete = r;
          });
          const unlistenChunk = await listen<number[]>("query-chunk", (e) => {
            chunks.push(e.payload);
          });
          const unlistenComplete = await listen("query-complete", () => {
            resolveComplete();
          });
          await streamQuery(sql);
          await completePromise;
          unlistenChunk();
          unlistenComplete();

          const elapsed = Math.round(performance.now() - start);
          let columns: string[] = [];
          const allRows: Record<string, unknown>[] = [];
          for (const chunk of chunks) {
            const decoded = decodeArrowIPC(chunk);
            if (decoded.columns.length) columns = decoded.columns;
            allRows.push(...decoded.rows);
          }
          const result = { columns, rows: allRows };
          if (tabId) updateTabState(tabId, { result, executionTimeMs: elapsed });
          setHistory((prev) => [
            { sql, timestamp: Date.now(), status: "success", executionTimeMs: elapsed },
            ...prev.slice(0, 19),
          ]);
        } else {
          const ipcData = await runPaginatedQuery(sql, 0, pageSize);
          const elapsed = Math.round(performance.now() - start);
          const decoded = decodeArrowIPC(ipcData);
          if (tabId) updateTabState(tabId, { result: decoded, executionTimeMs: elapsed });
          setHistory((prev) => [
            { sql, timestamp: Date.now(), status: "success", executionTimeMs: elapsed },
            ...prev.slice(0, 19),
          ]);
        }
      } catch (e) {
        const elapsed = Math.round(performance.now() - start);
        const parsed = extractError(e);
        if (tabId) updateTabState(tabId, { error: parsed, executionTimeMs: elapsed });
        setHistory((prev) => [
          { sql, timestamp: Date.now(), status: "error", executionTimeMs: elapsed, error: parsed.message },
          ...prev.slice(0, 19),
        ]);
      } finally {
        runningRef.current = false;
        setLoading(false);
      }
    },
    [updateTabState, pageSize]
  );

  const changePage = useCallback(
    async (newPage: number) => {
      const tabId = activeTabIdRef.current;
      if (!tabId) return;
      const tabState = tabStates[tabId];
      if (!tabState?.lastSql) return;
      setLoading(true);
      updateTabState(tabId, { error: null });
      try {
        const ipcData = await runPaginatedQuery(tabState.lastSql, newPage, pageSize);
        const decoded = decodeArrowIPC(ipcData);
        updateTabState(tabId, { result: decoded, page: newPage });
      } catch (e) {
        updateTabState(tabId, { error: extractError(e) });
      } finally {
        setLoading(false);
      }
    },
    [pageSize, updateTabState, tabStates]
  );

  const currentTabState = useMemo(
    () => (activeTabId ? tabStates[activeTabId] ?? EMPTY_TAB_STATE : EMPTY_TAB_STATE),
    [activeTabId, tabStates]
  );

  const changePageSize = useCallback(
    async (newPageSize: number) => {
      setPageSize(newPageSize);
      const tabId = activeTabIdRef.current;
      if (!tabId) return;
      const tabState = tabStates[tabId];
      if (!tabState?.lastSql) return;
      setLoading(true);
      updateTabState(tabId, { error: null, page: 0 });
      try {
        const ipcData = await runPaginatedQuery(tabState.lastSql, 0, newPageSize);
        const decoded = decodeArrowIPC(ipcData);
        updateTabState(tabId, { result: decoded });
      } catch (e) {
        updateTabState(tabId, { error: extractError(e) });
      } finally {
        setLoading(false);
      }
    },
    [updateTabState, tabStates]
  );

  const clearTabState = useCallback((tabId: string) => {
    setTabStates((prev) => {
      const next = { ...prev };
      delete next[tabId];
      return next;
    });
  }, []);

  return {
    result: currentTabState.result,
    loading,
    error: currentTabState.error,
    executionTimeMs: currentTabState.executionTimeMs,
    page: currentTabState.page,
    pageSize,
    history,
    executeQuery,
    changePage,
    changePageSize,
    setActiveQueryTabId,
    clearTabState,
  };
}
