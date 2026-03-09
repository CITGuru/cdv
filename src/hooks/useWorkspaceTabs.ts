import { useState, useCallback, useEffect, useRef } from "react";
import type { DataSource } from "@/lib/types";

export type ViewMode = "first100" | "last100" | "all" | "filtered";

export type DataTab = {
  id: string;
  type: "data";
  dataSourceId: string;
  viewMode?: ViewMode;
};

export type QueryTab = {
  id: string;
  type: "query";
  name: string;
  initialSql?: string;
  autoExecute?: boolean;
};

export type OpenQueryTabOptions = {
  autoExecute?: boolean;
  name?: string;
};

export type WorkspaceTab = DataTab | QueryTab;

export function useWorkspaceTabs() {
  const [openTabs, setOpenTabs] = useState<WorkspaceTab[]>([]);
  const [activeTabId, setActiveTabIdState] = useState<string | null>(null);
  const nextUntitledRef = useRef(1);

  const openDataTab = useCallback((ds: DataSource, viewMode?: ViewMode) => {
    const existing = openTabs.find(
      (t) => t.type === "data" && t.dataSourceId === ds.id
    );
    if (existing) {
      setActiveTabIdState(existing.id);
      setOpenTabs((prev) =>
        prev.map((t) =>
          t.type === "data" && t.id === existing.id
            ? { ...t, viewMode: viewMode ?? t.viewMode }
            : t
        )
      );
      return existing.id;
    }
    const id = `data-${ds.id}`;
    setOpenTabs((prev) => [...prev, { id, type: "data", dataSourceId: ds.id, viewMode }]);
    setActiveTabIdState(id);
    return id;
  }, [openTabs]);

  const openQueryTab = useCallback(
    (initialSql?: string, options?: OpenQueryTabOptions) => {
      const id = `query-${Date.now()}`;
      const name =
        options?.name != null && options.name !== ""
          ? options.name
          : `Untitled Query #${nextUntitledRef.current++}`;
      setOpenTabs((prev) => [
        ...prev,
        {
          id,
          type: "query",
          name,
          initialSql,
          autoExecute: options?.autoExecute,
        },
      ]);
      setActiveTabIdState(id);
      return id;
    },
    []
  );

  const updateTab = useCallback((id: string, patch: Partial<QueryTab>) => {
    setOpenTabs((prev) =>
      prev.map((t) => (t.type === "query" && t.id === id ? { ...t, ...patch } : t))
    );
  }, []);

  const closeTab = useCallback((id: string) => {
    setOpenTabs((prev) => prev.filter((t) => t.id !== id));
    setActiveTabIdState((current) => (current === id ? null : current));
  }, []);

  useEffect(() => {
    setActiveTabIdState((current) => {
      if (current == null) return openTabs[0]?.id ?? null;
      const exists = openTabs.some((t) => t.id === current);
      return exists ? current : openTabs[0]?.id ?? null;
    });
  }, [openTabs]);

  const setActiveTab = useCallback((id: string) => {
    setActiveTabIdState(id);
  }, []);

  const closeTabsForDataSource = useCallback((dataSourceId: string) => {
    setOpenTabs((prev) => {
      const next = prev.filter(
        (t) => t.type !== "data" || t.dataSourceId !== dataSourceId
      );
      const wasActive =
        activeTabId &&
        prev.some(
          (t) => t.id === activeTabId && t.type === "data" && t.dataSourceId === dataSourceId
        );
      if (wasActive) {
        const other = next.find((t) => t.id !== activeTabId);
        setActiveTabIdState(other?.id ?? null);
      }
      return next;
    });
  }, [activeTabId]);

  const activeTab = openTabs.find((t) => t.id === activeTabId) ?? null;

  const hydrateTabs = useCallback((openTabs: WorkspaceTab[], activeTabId: string | null) => {
    setOpenTabs(openTabs);
    setActiveTabIdState(activeTabId ?? null);
  }, []);

  return {
    openTabs,
    activeTabId,
    activeTab,
    setActiveTabId: setActiveTabIdState,
    hydrateTabs,
    openDataTab,
    openQueryTab,
    updateTab,
    closeTab,
    setActiveTab,
    closeTabsForDataSource,
  };
}
