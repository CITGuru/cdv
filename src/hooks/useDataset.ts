import { useState, useCallback, useRef } from "react";
import type { DataSource, QueryResult, PaginationState } from "../lib/types";
import type { ParsedError } from "../lib/errors";
import { extractError } from "../lib/errors";
import { runPaginatedQuery } from "../lib/ipc";
import { decodeArrowIPC } from "../lib/arrow";
import type { ViewMode } from "./useWorkspaceTabs";

const DEFAULT_PAGE_SIZE = 100;
const VIEW_MODE_PAGE_SIZE = 100;
const ALL_ROWS_CAP = 10_000;
const MAX_CACHE_ENTRIES = 30;

function cacheKey(dsId: string, viewMode: string | undefined, page: number, pageSize: number): string {
  return `${dsId}:${viewMode ?? "default"}:${page}:${pageSize}`;
}

function getInitialPageAndSize(
  ds: DataSource,
  viewMode?: ViewMode
): { page: number; pageSize: number } {
  const total = ds.row_count ?? 0;
  switch (viewMode) {
    case "first100":
      return { page: 0, pageSize: VIEW_MODE_PAGE_SIZE };
    case "last100": {
      const page = total <= 0 ? 0 : Math.max(0, Math.ceil(total / VIEW_MODE_PAGE_SIZE) - 1);
      return { page, pageSize: VIEW_MODE_PAGE_SIZE };
    }
    case "all":
      return { page: 0, pageSize: Math.min(total || ALL_ROWS_CAP, ALL_ROWS_CAP) || 1000 };
    case "filtered":
      return { page: 0, pageSize: VIEW_MODE_PAGE_SIZE };
    default:
      return { page: 0, pageSize: DEFAULT_PAGE_SIZE };
  }
}

export function useDataset() {
  const [dataSources, setDataSources] = useState<DataSource[]>([]);
  const [activeSource, setActiveSource] = useState<DataSource | null>(null);
  const [previewData, setPreviewData] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ParsedError | null>(null);
  const [pagination, setPagination] = useState<PaginationState>({
    page: 0,
    pageSize: DEFAULT_PAGE_SIZE,
    totalRows: null,
  });
  const resultCache = useRef<Map<string, QueryResult>>(new Map());

  const evictCache = useCallback(() => {
    const cache = resultCache.current;
    if (cache.size <= MAX_CACHE_ENTRIES) return;
    const keys = Array.from(cache.keys());
    const toRemove = keys.length - MAX_CACHE_ENTRIES;
    for (let i = 0; i < toRemove; i++) {
      cache.delete(keys[i]);
    }
  }, []);

  const invalidateCacheForSource = useCallback((dsId: string) => {
    const cache = resultCache.current;
    for (const key of cache.keys()) {
      if (key.startsWith(`${dsId}:`)) cache.delete(key);
    }
  }, []);

  const addDataSource = useCallback((ds: DataSource) => {
    setDataSources((prev) => {
      if (prev.find((d) => d.id === ds.id)) return prev;
      return [...prev, ds];
    });
  }, []);

  const selectSource = useCallback(async (ds: DataSource, viewMode?: ViewMode) => {
    setError(null);
    setActiveSource(ds);
    const { page: initialPage, pageSize: initialPageSize } = getInitialPageAndSize(ds, viewMode);
    setPagination((p) => ({
      ...p,
      page: initialPage,
      pageSize: initialPageSize,
      totalRows: ds.row_count,
    }));

    const key = cacheKey(ds.id, viewMode, initialPage, initialPageSize);
    const cached = resultCache.current.get(key);
    if (cached) {
      setPreviewData(cached);
      return;
    }

    setLoading(true);
    try {
      const sql = `SELECT * FROM "${ds.view_name}"`;
      const ipcData = await runPaginatedQuery(sql, initialPage, initialPageSize);
      const result = decodeArrowIPC(ipcData);
      resultCache.current.set(key, result);
      evictCache();
      setPreviewData(result);
    } catch (e) {
      setError(extractError(e));
    } finally {
      setLoading(false);
    }
  }, [evictCache]);

  const removeSource = useCallback(
    (id: string) => {
      setDataSources((prev) => prev.filter((d) => d.id !== id));
      invalidateCacheForSource(id);
      if (activeSource?.id === id) {
        setActiveSource(null);
        setPreviewData(null);
      }
    },
    [activeSource, invalidateCacheForSource]
  );

  const updateDataSource = useCallback((updated: DataSource) => {
    setDataSources((prev) =>
      prev.map((d) => (d.id === updated.id ? updated : d))
    );
    invalidateCacheForSource(updated.id);
    if (activeSource?.id === updated.id) {
      setActiveSource(updated);
    }
  }, [activeSource?.id, invalidateCacheForSource]);

  const changePage = useCallback(
    async (page: number) => {
      if (!activeSource) return;

      const key = cacheKey(activeSource.id, undefined, page, pagination.pageSize);
      const cached = resultCache.current.get(key);
      if (cached) {
        setPreviewData(cached);
        setPagination((p) => ({ ...p, page }));
        return;
      }

      setLoading(true);
      setError(null);
      try {
        const sql = `SELECT * FROM "${activeSource.view_name}"`;
        const ipcData = await runPaginatedQuery(sql, page, pagination.pageSize);
        const result = decodeArrowIPC(ipcData);
        resultCache.current.set(key, result);
        evictCache();
        setPreviewData(result);
        setPagination((p) => ({ ...p, page }));
      } catch (e) {
        setError(extractError(e));
      } finally {
        setLoading(false);
      }
    },
    [activeSource, pagination.pageSize, evictCache]
  );

  const clearSourceView = useCallback((dsId: string) => {
    invalidateCacheForSource(dsId);
    if (activeSource?.id === dsId) {
      setActiveSource(null);
      setPreviewData(null);
    }
  }, [activeSource?.id, invalidateCacheForSource]);

  const changePageSize = useCallback(
    async (pageSize: number) => {
      if (!activeSource) return;

      const key = cacheKey(activeSource.id, undefined, 0, pageSize);
      const cached = resultCache.current.get(key);
      if (cached) {
        setPreviewData(cached);
        setPagination((p) => ({ ...p, page: 0, pageSize }));
        return;
      }

      setLoading(true);
      setError(null);
      try {
        const sql = `SELECT * FROM "${activeSource.view_name}"`;
        const ipcData = await runPaginatedQuery(sql, 0, pageSize);
        const result = decodeArrowIPC(ipcData);
        resultCache.current.set(key, result);
        evictCache();
        setPreviewData(result);
        setPagination((p) => ({ ...p, page: 0, pageSize }));
      } catch (e) {
        setError(extractError(e));
      } finally {
        setLoading(false);
      }
    },
    [activeSource, evictCache]
  );

  return {
    dataSources,
    activeSource,
    previewData,
    loading,
    error,
    pagination,
    addDataSource,
    selectSource,
    removeSource,
    updateDataSource,
    clearSourceView,
    changePage,
    changePageSize,
    setDataSources,
  };
}
