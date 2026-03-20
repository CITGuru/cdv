import { useState, useCallback, useEffect, useRef } from "react";
import { listen } from "@tauri-apps/api/event";
import type {
  EtlJob,
  EtlProgressEvent,
  EtlCompleteEvent,
  SyncStrategy,
  CatalogEntry,
} from "../lib/types";
import {
  createEtlJob as createEtlJobIpc,
  listEtlJobs as listEtlJobsIpc,
  getEtlJob as getEtlJobIpc,
  deleteEtlJob as deleteEtlJobIpc,
  runEtlJob as runEtlJobIpc,
  cancelEtlJob as cancelEtlJobIpc,
  previewEtlJob as previewEtlJobIpc,
} from "../lib/ipc";

export function useEtlJobs() {
  const [jobs, setJobs] = useState<EtlJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [activeProgress, setActiveProgress] = useState<EtlProgressEvent | null>(null);
  const [lastComplete, setLastComplete] = useState<EtlCompleteEvent | null>(null);
  const loadedRef = useRef(false);

  const loadJobs = useCallback(async () => {
    if (loadedRef.current) return;
    setLoading(true);
    try {
      const list = await listEtlJobsIpc();
      setJobs(list);
      loadedRef.current = true;
    } catch {
      // silently fail
    } finally {
      setLoading(false);
    }
  }, []);

  const refreshJobs = useCallback(async () => {
    try {
      const list = await listEtlJobsIpc();
      setJobs(list);
    } catch {
      // silently fail
    }
  }, []);

  const createJob = useCallback(
    async (params: {
      name: string;
      sourceConnectorId: string;
      targetConnectorId: string;
      strategy: SyncStrategy;
      includeSchemas?: string[];
      excludeTables?: string[];
      skipViews?: boolean;
      batchSize?: number;
    }): Promise<EtlJob> => {
      const job = await createEtlJobIpc(params);
      setJobs((prev) => [...prev, job]);
      return job;
    },
    []
  );

  const runJob = useCallback(async (jobId: string) => {
    await runEtlJobIpc(jobId);
    setJobs((prev) =>
      prev.map((j) => (j.id === jobId ? { ...j, status: "running" as const } : j))
    );
    setActiveProgress(null);
    setLastComplete(null);
  }, []);

  const cancelJob = useCallback(async () => {
    await cancelEtlJobIpc();
  }, []);

  const deleteJob = useCallback(async (jobId: string) => {
    await deleteEtlJobIpc(jobId);
    setJobs((prev) => prev.filter((j) => j.id !== jobId));
  }, []);

  const getJob = useCallback(async (jobId: string) => {
    return getEtlJobIpc(jobId);
  }, []);

  const previewJob = useCallback(
    async (params: {
      sourceConnectorId: string;
      includeSchemas?: string[];
      excludeTables?: string[];
      skipViews?: boolean;
    }): Promise<CatalogEntry[]> => {
      return previewEtlJobIpc(params);
    },
    []
  );

  useEffect(() => {
    const unlistenProgress = listen<EtlProgressEvent>("etl-progress", (event) => {
      setActiveProgress(event.payload);
    });

    const unlistenComplete = listen<EtlCompleteEvent>("etl-complete", (event) => {
      setLastComplete(event.payload);
      setActiveProgress(null);
      refreshJobs();
    });

    return () => {
      unlistenProgress.then((fn) => fn());
      unlistenComplete.then((fn) => fn());
    };
  }, [refreshJobs]);

  const runningJob = jobs.find((j) => j.status === "running") ?? null;

  return {
    jobs,
    loading,
    activeProgress,
    lastComplete,
    runningJob,
    loadJobs,
    refreshJobs,
    createJob,
    runJob,
    cancelJob,
    deleteJob,
    getJob,
    previewJob,
    setLastComplete,
  };
}
