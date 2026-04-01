import { useState, useCallback, useRef, useMemo } from "react";
import type {
  Connector,
  ConnectorType,
  ConnectorConfig,
  ConnectorBrowseCache,
  CatalogEntry,
  AddConnectorResult,
} from "../lib/types";
import {
  addConnector as addConnectorIpc,
  removeConnector as removeConnectorIpc,
  listConnectors as listConnectorsIpc,
  introspectConnector as introspectConnectorIpc,
  testConnector as testConnectorIpc,
  getCachedCatalogs as getCachedCatalogsIpc,
  listConnectorDatabases as listConnectorDatabasesIpc,
  connectConnectorDatabase as connectConnectorDatabaseIpc,
} from "../lib/ipc";

const DB_CONNECTOR_TYPES: ConnectorType[] = [
  "sqlite",
  "duckdb",
  "postgresql",
  "snowflake",
  "ducklake",
];

function isDbConnector(ct: ConnectorType): boolean {
  return DB_CONNECTOR_TYPES.includes(ct);
}

function isMultiDbConnector(ct: ConnectorType): boolean {
  return ct === "postgresql" || ct === "snowflake";
}

export function useConnectors() {
  const [connectors, setConnectors] = useState<Connector[]>([]);
  /** Per-connector browse cache from the backend (nested catalogs for PG/SF). */
  const [connectorBrowse, setConnectorBrowse] = useState<
    Record<string, ConnectorBrowseCache>
  >({});
  const [catalogLoading, setCatalogLoading] = useState<Record<string, boolean>>(
    {}
  );
  /** True while a full server database list refresh runs (Snowflake deep scan). */
  const [dbListLoading, setDbListLoading] = useState<Record<string, boolean>>(
    {}
  );
  const [loading, setLoading] = useState(false);
  const loadedRef = useRef(false);

  const refreshBrowseFromServer = useCallback(async () => {
    try {
      const cached = await getCachedCatalogsIpc();
      if (cached && Object.keys(cached).length > 0) {
        setConnectorBrowse(cached);
      }
    } catch {
      // ignore
    }
  }, []);

  const loadConnectors = useCallback(async () => {
    if (loadedRef.current) return;
    setLoading(true);
    try {
      const list = await listConnectorsIpc();
      setConnectors(list);
      loadedRef.current = true;
      await refreshBrowseFromServer();
    } catch {
      // silently fail
    } finally {
      setLoading(false);
    }
  }, [refreshBrowseFromServer]);

  const loadCatalog = useCallback(
    async (connectorId: string) => {
      setCatalogLoading((prev) => ({ ...prev, [connectorId]: true }));
      try {
        const c = connectors.find((x) => x.id === connectorId);
        await introspectConnectorIpc(connectorId);
        if (c && isMultiDbConnector(c.connector_type)) {
          await listConnectorDatabasesIpc(connectorId, { refresh: false });
        }
        await refreshBrowseFromServer();
        if (c && isMultiDbConnector(c.connector_type)) {
          const id = connectorId;
          setDbListLoading((prev) => ({ ...prev, [id]: true }));
          window.setTimeout(() => {
            listConnectorDatabasesIpc(id, { refresh: true })
              .then(() => refreshBrowseFromServer())
              .catch(() => {})
              .finally(() =>
                setDbListLoading((prev) => ({ ...prev, [id]: false }))
              );
          }, 200);
        }
      } catch {
        await refreshBrowseFromServer();
      } finally {
        setCatalogLoading((prev) => ({ ...prev, [connectorId]: false }));
      }
    },
    [connectors, refreshBrowseFromServer]
  );

  const addConnector = useCallback(
    async (params: {
      name: string;
      connectorType: ConnectorType;
      config: ConnectorConfig;
      accessKey?: string;
      secretKey?: string;
    }): Promise<AddConnectorResult> => {
      const connector = await addConnectorIpc(params);
      setConnectors((prev) => [...prev, connector]);

      let introspectedCatalog: CatalogEntry[] | undefined;
      if (isDbConnector(connector.connector_type)) {
        try {
          introspectedCatalog = await introspectConnectorIpc(connector.id);
          if (isMultiDbConnector(connector.connector_type)) {
            await listConnectorDatabasesIpc(connector.id, { refresh: false });
          }
          await refreshBrowseFromServer();
        } catch {
          await refreshBrowseFromServer();
        }
      }

      return { connector, introspectedCatalog };
    },
    [refreshBrowseFromServer]
  );

  const removeConnector = useCallback(
    async (id: string) => {
      await removeConnectorIpc(id);
      setConnectors((prev) => prev.filter((c) => c.id !== id));
      setConnectorBrowse((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      setDbListLoading((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
    },
    []
  );

  const refreshCatalog = useCallback(
    async (connectorId: string) => {
      setCatalogLoading((prev) => ({ ...prev, [connectorId]: true }));
      try {
        const c = connectors.find((x) => x.id === connectorId);
        if (c && isMultiDbConnector(c.connector_type)) {
          setDbListLoading((prev) => ({ ...prev, [connectorId]: true }));
          try {
            await listConnectorDatabasesIpc(connectorId, { refresh: true });
          } finally {
            setDbListLoading((prev) => ({ ...prev, [connectorId]: false }));
          }
        }
        await introspectConnectorIpc(connectorId);
        await refreshBrowseFromServer();
        const b = await getCachedCatalogsIpc();
        const entries =
          b[connectorId]?.catalogs_by_database[
            b[connectorId]?.default_database ?? ""
          ] ?? [];
        return entries;
      } catch {
        await refreshBrowseFromServer();
        return [];
      } finally {
        setCatalogLoading((prev) => ({ ...prev, [connectorId]: false }));
      }
    },
    [connectors, refreshBrowseFromServer]
  );

  const connectDatabase = useCallback(
    async (connectorId: string, database: string) => {
      await connectConnectorDatabaseIpc(connectorId, database);
      await refreshBrowseFromServer();
    },
    [refreshBrowseFromServer]
  );

  const testConnection = useCallback(
    async (params: {
      connectorType: ConnectorType;
      config: ConnectorConfig;
      accessKey?: string;
      secretKey?: string;
    }) => {
      await testConnectorIpc(params);
    },
    []
  );

  const getConnectorById = useCallback(
    (id: string): Connector | undefined => {
      return connectors.find((c) => c.id === id);
    },
    [connectors]
  );

  /** Flat catalog for default database (sidebar single-level connectors). */
  const catalogs = useMemo((): Record<string, CatalogEntry[]> => {
    const out: Record<string, CatalogEntry[]> = {};
    for (const c of connectors) {
      const b = connectorBrowse[c.id];
      if (!b) {
        out[c.id] = [];
        continue;
      }
      const key = b.default_database;
      out[c.id] = b.catalogs_by_database[key] ?? [];
    }
    return out;
  }, [connectors, connectorBrowse]);

  return {
    connectors,
    connectorBrowse,
    catalogs,
    catalogLoading,
    dbListLoading,
    loading,
    loadConnectors,
    loadCatalog,
    addConnector,
    removeConnector,
    refreshCatalog,
    connectDatabase,
    testConnection,
    getConnectorById,
    setConnectors,
    refreshBrowseFromServer,
  };
}
