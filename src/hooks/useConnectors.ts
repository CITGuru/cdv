import { useState, useCallback, useRef } from "react";
import type {
  Connector,
  ConnectorType,
  ConnectorConfig,
  CatalogEntry,
} from "../lib/types";
import {
  addConnector as addConnectorIpc,
  removeConnector as removeConnectorIpc,
  listConnectors as listConnectorsIpc,
  introspectConnector as introspectConnectorIpc,
  testConnector as testConnectorIpc,
  getCachedCatalogs as getCachedCatalogsIpc,
} from "../lib/ipc";

const DB_CONNECTOR_TYPES: ConnectorType[] = ["sqlite", "duckdb", "postgresql", "snowflake", "ducklake"];

function isDbConnector(ct: ConnectorType): boolean {
  return DB_CONNECTOR_TYPES.includes(ct);
}

export function useConnectors() {
  const [connectors, setConnectors] = useState<Connector[]>([]);
  const [catalogs, setCatalogs] = useState<Record<string, CatalogEntry[]>>({});
  const [catalogLoading, setCatalogLoading] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(false);
  const loadedRef = useRef(false);

  const loadConnectors = useCallback(async () => {
    if (loadedRef.current) return;
    setLoading(true);
    try {
      const list = await listConnectorsIpc();
      setConnectors(list);
      loadedRef.current = true;

      try {
        const cached = await getCachedCatalogsIpc();
        if (cached && Object.keys(cached).length > 0) {
          setCatalogs(cached);
        }
      } catch {
        // no cached data available
      }
    } catch {
      // silently fail
    } finally {
      setLoading(false);
    }
  }, []);

  const loadCatalog = useCallback(async (connectorId: string) => {
    setCatalogLoading((prev) => ({ ...prev, [connectorId]: true }));
    try {
      const entries = await introspectConnectorIpc(connectorId);
      setCatalogs((prev) => ({ ...prev, [connectorId]: entries }));
    } catch {
      setCatalogs((prev) => ({ ...prev, [connectorId]: [] }));
    } finally {
      setCatalogLoading((prev) => ({ ...prev, [connectorId]: false }));
    }
  }, []);

  const addConnector = useCallback(
    async (params: {
      name: string;
      connectorType: ConnectorType;
      config: ConnectorConfig;
      accessKey?: string;
      secretKey?: string;
    }): Promise<Connector> => {
      const connector = await addConnectorIpc(params);
      setConnectors((prev) => [...prev, connector]);

      if (isDbConnector(connector.connector_type)) {
        try {
          const entries = await introspectConnectorIpc(connector.id);
          setCatalogs((prev) => ({ ...prev, [connector.id]: entries }));
        } catch {
          setCatalogs((prev) => ({ ...prev, [connector.id]: [] }));
        }
      }

      return connector;
    },
    []
  );

  const removeConnector = useCallback(async (id: string) => {
    await removeConnectorIpc(id);
    setConnectors((prev) => prev.filter((c) => c.id !== id));
    setCatalogs((prev) => {
      const next = { ...prev };
      delete next[id];
      return next;
    });
  }, []);

  const refreshCatalog = useCallback(async (connectorId: string) => {
    setCatalogLoading((prev) => ({ ...prev, [connectorId]: true }));
    try {
      const entries = await introspectConnectorIpc(connectorId);
      setCatalogs((prev) => ({ ...prev, [connectorId]: entries }));
      return entries;
    } catch {
      setCatalogs((prev) => ({ ...prev, [connectorId]: [] }));
      return [];
    } finally {
      setCatalogLoading((prev) => ({ ...prev, [connectorId]: false }));
    }
  }, []);

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

  return {
    connectors,
    catalogs,
    catalogLoading,
    loading,
    loadConnectors,
    loadCatalog,
    addConnector,
    removeConnector,
    refreshCatalog,
    testConnection,
    getConnectorById,
    setConnectors,
  };
}
