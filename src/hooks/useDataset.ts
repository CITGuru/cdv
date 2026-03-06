import { useState, useCallback } from "react";
import type { DatasetInfo, QueryResult, PaginationState } from "../lib/types";
import { registerDataset, getPreview, runPaginatedQuery } from "../lib/ipc";
import { decodeArrowIPC } from "../lib/arrow";

export function useDataset() {
  const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
  const [activeDataset, setActiveDataset] = useState<DatasetInfo | null>(null);
  const [previewData, setPreviewData] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pagination, setPagination] = useState<PaginationState>({
    page: 0,
    pageSize: 1000,
    totalRows: null,
  });

  const openFile = useCallback(async (path: string) => {
    setLoading(true);
    setError(null);
    try {
      const dataset = await registerDataset(path);
      setDatasets((prev) => {
        const exists = prev.find((d) => d.id === dataset.id);
        if (exists) return prev;
        return [...prev, dataset];
      });
      setActiveDataset(dataset);
      setPagination((p) => ({
        ...p,
        page: 0,
        totalRows: dataset.row_count,
      }));

      const ipcData = await getPreview(dataset.id);
      const result = decodeArrowIPC(ipcData);
      setPreviewData(result);
    } catch (e) {
      setError(typeof e === "string" ? e : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  const selectDataset = useCallback(
    async (dataset: DatasetInfo) => {
      setLoading(true);
      setError(null);
      setActiveDataset(dataset);
      setPagination((p) => ({
        ...p,
        page: 0,
        totalRows: dataset.row_count,
      }));
      try {
        const ipcData = await getPreview(dataset.id);
        const result = decodeArrowIPC(ipcData);
        setPreviewData(result);
      } catch (e) {
        setError(typeof e === "string" ? e : String(e));
      } finally {
        setLoading(false);
      }
    },
    []
  );

  const changePage = useCallback(
    async (page: number) => {
      if (!activeDataset) return;
      setLoading(true);
      setError(null);
      try {
        const sql = `SELECT * FROM ${activeDataset.duckdb_ref}`;
        const ipcData = await runPaginatedQuery(sql, page, pagination.pageSize);
        const result = decodeArrowIPC(ipcData);
        setPreviewData(result);
        setPagination((p) => ({ ...p, page }));
      } catch (e) {
        setError(typeof e === "string" ? e : String(e));
      } finally {
        setLoading(false);
      }
    },
    [activeDataset, pagination.pageSize]
  );

  const changePageSize = useCallback(
    async (pageSize: number) => {
      if (!activeDataset) return;
      setLoading(true);
      setError(null);
      try {
        const sql = `SELECT * FROM ${activeDataset.duckdb_ref}`;
        const ipcData = await runPaginatedQuery(sql, 0, pageSize);
        const result = decodeArrowIPC(ipcData);
        setPreviewData(result);
        setPagination((p) => ({ ...p, page: 0, pageSize }));
      } catch (e) {
        setError(typeof e === "string" ? e : String(e));
      } finally {
        setLoading(false);
      }
    },
    [activeDataset]
  );

  return {
    datasets,
    activeDataset,
    previewData,
    loading,
    error,
    pagination,
    openFile,
    selectDataset,
    changePage,
    changePageSize,
    setDatasets,
  };
}
