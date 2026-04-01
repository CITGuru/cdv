import { memo, useCallback, useEffect, useState } from "react";
import {
  Trash2,
  Table2,
  TerminalSquare,
  Download,
  ChevronRight,
  ChevronDown,
  FolderOpen,
  Folder,
  RefreshCw,
  Database,
  Link2,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from "@/components/ui/context-menu";
import type {
  DataSource,
  Connector,
  CatalogEntry,
  ConnectorBrowseCache,
} from "@/lib/types";
import type { ViewMode } from "@/hooks/useWorkspaceTabs";
import { getConnectorIcon, getConnectorLabel, getConnectorIconColor } from "./sidebar-icons";
import { ColumnTreeRow } from "./ColumnTreeRow";

function sortDatabaseNames(browse: ConnectorBrowseCache): string[] {
  const def = browse.default_database;
  const seen = new Set<string>();
  const out: string[] = [];
  if (def && browse.database_names.includes(def)) {
    out.push(def);
    seen.add(def);
  }
  for (const n of [...browse.database_names].sort()) {
    if (!seen.has(n)) {
      out.push(n);
      seen.add(n);
    }
  }
  if (def && !seen.has(def)) {
    out.unshift(def);
  }
  return out;
}

function isMultiDbConnector(c: Connector): boolean {
  return c.connector_type === "postgresql" || c.connector_type === "snowflake";
}

export const DatabaseConnectorItem = memo(function DatabaseConnectorItem({
  connector,
  browse,
  catalog,
  catalogIsLoading,
  dataSources,
  isExpanded,
  onToggleExpand,
  isIdExpanded,
  onToggleSchemaExpand,
  onRemove,
  onRefresh,
  onImportTable,
  onConnectDatabase,
  onNewQuery,
  onDataSourceSelect,
  activeSourceId,
  databaseListLoading,
}: {
  connector: Connector;
  browse?: ConnectorBrowseCache | null;
  catalog: CatalogEntry[];
  catalogIsLoading: boolean;
  /** Full server database list refresh in progress (e.g. Snowflake SCHEMATA scan). */
  databaseListLoading?: boolean;
  dataSources: DataSource[];
  isExpanded: boolean;
  onToggleExpand: () => void;
  isIdExpanded: (id: string) => boolean;
  onToggleSchemaExpand: (id: string) => void;
  onRemove: (id: string) => void;
  onRefresh?: (id: string) => void;
  onImportTable?: (
    connectorId: string,
    database: string | undefined,
    schema: string,
    table: string
  ) => void;
  onConnectDatabase?: (connectorId: string, database: string) => Promise<void>;
  onNewQuery?: (qualifiedName: string) => void;
  onDataSourceSelect?: (ds: DataSource, viewMode?: ViewMode) => void;
  activeSourceId: string | null;
}) {
  const [connectingDb, setConnectingDb] = useState<string | null>(null);
  /** Expanded state for database rows (PG/SF); default DB starts expanded when catalog exists. */
  const [dbExpandedLocal, setDbExpandedLocal] = useState<Record<string, boolean>>({});

  const Icon = getConnectorIcon(connector);
  const label = getConnectorLabel(connector);
  const iconColor = getConnectorIconColor(connector);

  const useMultiDbUi =
    isMultiDbConnector(connector) &&
    browse &&
    browse.database_names &&
    browse.database_names.length > 0;

  useEffect(() => {
    if (!browse || !useMultiDbUi) return;
    const def = browse.default_database;
    if (!Object.prototype.hasOwnProperty.call(browse.catalogs_by_database, def)) return;
    setDbExpandedLocal((p) => {
      if (p[def] !== undefined) return p;
      return { ...p, [def]: true };
    });
  }, [browse, useMultiDbUi]);

  const isDbRowExpanded = (dbName: string) => dbExpandedLocal[dbName] ?? false;
  const toggleDbRow = (dbName: string) => {
    setDbExpandedLocal((p) => ({ ...p, [dbName]: !(p[dbName] ?? false) }));
  };

  const schemasFromCatalog = useCallback((entries: CatalogEntry[]) => {
    const schemas = new Map<string, CatalogEntry[]>();
    for (const entry of entries) {
      const trimmed = entry.schema?.trim();
      const s = trimmed && trimmed.length > 0 ? trimmed : "default";
      if (!schemas.has(s)) schemas.set(s, []);
      schemas.get(s)!.push(entry);
    }
    return schemas;
  }, []);

  const renderSchemaTables = (
    entries: CatalogEntry[],
    databaseName: string | undefined,
    attachAlias: string
  ) => {
    const schemas = schemasFromCatalog(entries);
    return Array.from(schemas.entries()).map(([schemaName, tables]) => {
      const schemaKey = `schema-${connector.id}-${databaseName ?? "x"}-${schemaName}`;
      const schemaExpanded = isIdExpanded(schemaKey);
      return (
        <div key={schemaName}>
          <button
            type="button"
            className="flex items-center gap-1.5 py-1 px-1.5 text-[11px] font-medium text-muted-foreground uppercase tracking-wider hover:text-sidebar-foreground w-full text-left"
            onClick={() => onToggleSchemaExpand(schemaKey)}
          >
            {schemaExpanded ? (
              <FolderOpen className="size-3.5 shrink-0" />
            ) : (
              <Folder className="size-3.5 shrink-0" />
            )}
            <span>{schemaName}</span>
            <span className="text-[10px] tabular-nums">{tables.length}</span>
          </button>
          {schemaExpanded &&
            tables.map((entry) => {
              const rowKey = `${schemaName}.${entry.name}`;
              const existingDs = dataSources.find(
                (ds) =>
                  ds.connector_id === connector.id &&
                  ds.qualified_name.includes(entry.name)
              );
              const tableKey = `table-${connector.id}-${databaseName ?? "x"}-${schemaName}-${entry.name}`;
              const tableExpanded = isIdExpanded(tableKey);
              const hasColumns = entry.columns && entry.columns.length > 0;
              return (
                <div key={rowKey} className="flex flex-col">
                  <ContextMenu>
                    <ContextMenuTrigger asChild>
                      <div
                        className={`flex items-center gap-0.5 py-0.5 pr-2 rounded text-xs min-w-0 w-full text-left hover:bg-sidebar-accent/50 ${
                          existingDs && activeSourceId === existingDs.id
                            ? "bg-sidebar-accent text-sidebar-accent-foreground"
                            : "text-sidebar-foreground/90"
                        }`}
                      >
                        <button
                          type="button"
                          className="shrink-0 p-0.5 rounded hover:bg-sidebar-accent/80 flex items-center justify-center w-5 ml-1"
                          onClick={(e) => {
                            e.stopPropagation();
                            if (hasColumns) onToggleSchemaExpand(tableKey);
                          }}
                        >
                          {hasColumns ? (
                            tableExpanded ? (
                              <ChevronDown className="size-3 text-muted-foreground" />
                            ) : (
                              <ChevronRight className="size-3 text-muted-foreground" />
                            )
                          ) : (
                            <span className="inline-block w-3" aria-hidden />
                          )}
                        </button>
                        <button
                          type="button"
                          className="flex-1 flex items-center gap-2 py-0.5 min-w-0 text-left"
                          onClick={() => {
                            if (existingDs && onDataSourceSelect) {
                              onDataSourceSelect(existingDs);
                            } else if (onImportTable) {
                              onImportTable(connector.id, databaseName, schemaName, entry.name);
                            }
                          }}
                        >
                          <Table2 className="size-3 shrink-0 text-muted-foreground/80" />
                          <span className="font-mono truncate text-[11px]">{entry.name}</span>
                          <span className="text-[10px] text-muted-foreground shrink-0">
                            {entry.entry_type}
                          </span>
                          {hasColumns && (
                            <span className="shrink-0 text-[10px] text-muted-foreground tabular-nums">
                              {entry.columns.length}
                            </span>
                          )}
                        </button>
                      </div>
                    </ContextMenuTrigger>
                    <ContextMenuContent className="w-48">
                      {onNewQuery && attachAlias && (
                        <ContextMenuItem
                          onSelect={() => {
                            const qn = `"${attachAlias}"."${schemaName}"."${entry.name}"`;
                            onNewQuery(qn);
                          }}
                        >
                          <TerminalSquare className="size-4 mr-2" />
                          New Query
                        </ContextMenuItem>
                      )}
                      {onImportTable && !existingDs && (
                        <ContextMenuItem
                          onSelect={() =>
                            onImportTable(connector.id, databaseName, schemaName, entry.name)
                          }
                        >
                          <Download className="size-4 mr-2" />
                          Import Table
                        </ContextMenuItem>
                      )}
                    </ContextMenuContent>
                  </ContextMenu>
                  {hasColumns && tableExpanded && (
                    <div className="ml-3 border-l border-border/50 flex flex-col py-0.5">
                      {entry.columns.map((col) => (
                        <ColumnTreeRow key={col.name} column={col} />
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
        </div>
      );
    });
  };

  const schemas = schemasFromCatalog(catalog);

  const handleConnectDb = async (db: string) => {
    if (!onConnectDatabase) return;
    setConnectingDb(db);
    try {
      await onConnectDatabase(connector.id, db);
    } finally {
      setConnectingDb(null);
    }
  };

  return (
    <div className="flex flex-col gap-0 rounded-md">
      <ContextMenu>
        <ContextMenuTrigger asChild>
          <div className="group flex items-center gap-0.5 rounded-md text-sidebar-foreground hover:bg-sidebar-accent/50 transition-colors min-h-7">
            <button
              type="button"
              className="shrink-0 p-0.5 rounded hover:bg-sidebar-accent/80 flex items-center justify-center w-5"
              onClick={(e) => {
                e.stopPropagation();
                onToggleExpand();
              }}
            >
              {isExpanded ? (
                <ChevronDown className="size-3.5 text-muted-foreground" />
              ) : (
                <ChevronRight className="size-3.5 text-muted-foreground" />
              )}
            </button>
            <div
              className="flex-1 flex items-center gap-2 px-1.5 py-1 min-w-0 cursor-pointer"
              onClick={onToggleExpand}
            >
              <Icon className={`size-4 shrink-0 ${iconColor}`} />
              <span className="truncate text-xs font-medium flex-1">{connector.name}</span>
              <span
                className={`shrink-0 text-[9px] px-1.5 py-0.5 rounded-sm font-medium bg-muted ${iconColor}`}
              >
                {label}
              </span>
            </div>
            <Button
              variant="ghost"
              size="icon-xs"
              className="opacity-0 group-hover:opacity-100 shrink-0 h-6 w-6"
              onClick={() => onRemove(connector.id)}
            >
              <Trash2 className="size-3 text-muted-foreground hover:text-destructive" />
            </Button>
          </div>
        </ContextMenuTrigger>
        <ContextMenuContent className="w-48">
          {onRefresh && (
            <ContextMenuItem onSelect={() => onRefresh(connector.id)}>
              <RefreshCw className="size-4 mr-2" />
              Refresh
            </ContextMenuItem>
          )}
          <ContextMenuSeparator />
          <ContextMenuItem variant="destructive" onSelect={() => onRemove(connector.id)}>
            <Trash2 className="size-4 mr-2" />
            Remove
          </ContextMenuItem>
        </ContextMenuContent>
      </ContextMenu>

      {isExpanded && (
        <div className="ml-2 pl-4 border-l border-border/50 flex flex-col py-0.5">
          {useMultiDbUi && browse ? (
            <>
              {catalogIsLoading && browse.database_names.length === 0 ? (
                <div className="flex items-center gap-2 px-3 py-2">
                  <RefreshCw className="size-3 animate-spin text-muted-foreground" />
                  <span className="text-[11px] text-muted-foreground">Loading…</span>
                </div>
              ) : (
                <>
                  <div className="flex items-center gap-1.5 py-1 px-1.5 text-[11px] font-medium text-muted-foreground uppercase tracking-wider">
                    <Database className="size-3.5 shrink-0" />
                    <span>Databases</span>
                    <span className="text-[10px] tabular-nums">
                      {browse.database_names.length}
                    </span>
                    {databaseListLoading ? (
                      <RefreshCw
                        className="size-3 shrink-0 animate-spin text-muted-foreground"
                        aria-label="Loading database list from server"
                      />
                    ) : null}
                  </div>
                  {sortDatabaseNames(browse).map((dbName) => {
                    const connected = Object.prototype.hasOwnProperty.call(
                      browse.catalogs_by_database,
                      dbName
                    );
                    const dbExpanded = isDbRowExpanded(dbName);
                    const entries = browse.catalogs_by_database[dbName] ?? [];
                    const attachAlias =
                      browse.attach_aliases_by_database[dbName] ??
                      connector.alias ??
                      "";
                    const isDefault = dbName === browse.default_database;

                    return (
                      <div key={dbName} className="ml-1">
                        <div className="flex items-center gap-0.5 py-0.5">
                          {connected ? (
                            <button
                              type="button"
                              className="shrink-0 p-0.5 rounded hover:bg-sidebar-accent/80 flex items-center justify-center w-5"
                              onClick={() => toggleDbRow(dbName)}
                            >
                              {dbExpanded ? (
                                <ChevronDown className="size-3 text-muted-foreground" />
                              ) : (
                                <ChevronRight className="size-3 text-muted-foreground" />
                              )}
                            </button>
                          ) : (
                            <span className="inline-block w-5" aria-hidden />
                          )}
                          <span className="flex-1 truncate text-[11px] font-mono text-sidebar-foreground/90 px-0.5">
                            {dbName}
                            {isDefault ? (
                              <span className="ml-1 text-[9px] text-muted-foreground font-sans">
                                default
                              </span>
                            ) : null}
                          </span>
                          {!connected && onConnectDatabase && (
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-6 px-1.5 text-[10px] shrink-0"
                              disabled={connectingDb === dbName}
                              onClick={() => handleConnectDb(dbName)}
                            >
                              {connectingDb === dbName ? (
                                <RefreshCw className="size-3 animate-spin" />
                              ) : (
                                <>
                                  <Link2 className="size-3 mr-0.5" />
                                  Connect
                                </>
                              )}
                            </Button>
                          )}
                        </div>
                        {connected && dbExpanded && (
                          <div className="ml-3 pl-2 border-l border-border/40">
                            {entries.length === 0 && !catalogIsLoading ? (
                              <p className="px-2 py-1 text-[10px] text-muted-foreground italic">
                                No tables found
                              </p>
                            ) : (
                              renderSchemaTables(entries, dbName, attachAlias)
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </>
              )}
            </>
          ) : catalogIsLoading && catalog.length === 0 ? (
            <div className="flex items-center gap-2 px-3 py-2">
              <RefreshCw className="size-3 animate-spin text-muted-foreground" />
              <span className="text-[11px] text-muted-foreground">Loading tables…</span>
            </div>
          ) : (
            Array.from(schemas.entries()).map(([schemaName, tables]) => {
              const schemaKey = `schema-${connector.id}-${schemaName}`;
              const schemaExpanded = isIdExpanded(schemaKey);
              const attachAlias = connector.alias ?? "";
              return (
                <div key={schemaName}>
                  <button
                    type="button"
                    className="flex items-center gap-1.5 py-1 px-1.5 text-[11px] font-medium text-muted-foreground uppercase tracking-wider hover:text-sidebar-foreground w-full text-left"
                    onClick={() => onToggleSchemaExpand(schemaKey)}
                  >
                    {schemaExpanded ? (
                      <FolderOpen className="size-3.5 shrink-0" />
                    ) : (
                      <Folder className="size-3.5 shrink-0" />
                    )}
                    <span>{schemaName}</span>
                    <span className="text-[10px] tabular-nums">{tables.length}</span>
                  </button>
                  {schemaExpanded &&
                    tables.map((entry) => {
                      const rowKey = `${schemaName}.${entry.name}`;
                      const existingDs = dataSources.find(
                        (ds) =>
                          ds.connector_id === connector.id &&
                          ds.qualified_name.includes(entry.name)
                      );
                      const tableKey = `table-${connector.id}-${schemaName}-${entry.name}`;
                      const tableExpanded = isIdExpanded(tableKey);
                      const hasColumns = entry.columns && entry.columns.length > 0;
                      return (
                        <div key={rowKey} className="flex flex-col">
                          <ContextMenu>
                            <ContextMenuTrigger asChild>
                              <div
                                className={`flex items-center gap-0.5 py-0.5 pr-2 rounded text-xs min-w-0 w-full text-left hover:bg-sidebar-accent/50 ${
                                  existingDs && activeSourceId === existingDs.id
                                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                                    : "text-sidebar-foreground/90"
                                }`}
                              >
                                <button
                                  type="button"
                                  className="shrink-0 p-0.5 rounded hover:bg-sidebar-accent/80 flex items-center justify-center w-5 ml-1"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    if (hasColumns) onToggleSchemaExpand(tableKey);
                                  }}
                                >
                                  {hasColumns ? (
                                    tableExpanded ? (
                                      <ChevronDown className="size-3 text-muted-foreground" />
                                    ) : (
                                      <ChevronRight className="size-3 text-muted-foreground" />
                                    )
                                  ) : (
                                    <span className="inline-block w-3" aria-hidden />
                                  )}
                                </button>
                                <button
                                  type="button"
                                  className="flex-1 flex items-center gap-2 py-0.5 min-w-0 text-left"
                                  onClick={() => {
                                    if (existingDs && onDataSourceSelect) {
                                      onDataSourceSelect(existingDs);
                                    } else if (onImportTable) {
                                      onImportTable(connector.id, undefined, schemaName, entry.name);
                                    }
                                  }}
                                >
                                  <Table2 className="size-3 shrink-0 text-muted-foreground/80" />
                                  <span className="font-mono truncate text-[11px]">{entry.name}</span>
                                  <span className="text-[10px] text-muted-foreground shrink-0">
                                    {entry.entry_type}
                                  </span>
                                  {hasColumns && (
                                    <span className="shrink-0 text-[10px] text-muted-foreground tabular-nums">
                                      {entry.columns.length}
                                    </span>
                                  )}
                                </button>
                              </div>
                            </ContextMenuTrigger>
                            <ContextMenuContent className="w-48">
                              {onNewQuery && attachAlias && (
                                <ContextMenuItem
                                  onSelect={() => {
                                    const qn = `"${attachAlias}"."${schemaName}"."${entry.name}"`;
                                    onNewQuery(qn);
                                  }}
                                >
                                  <TerminalSquare className="size-4 mr-2" />
                                  New Query
                                </ContextMenuItem>
                              )}
                              {onImportTable && !existingDs && (
                                <ContextMenuItem
                                  onSelect={() =>
                                    onImportTable(
                                      connector.id,
                                      undefined,
                                      schemaName,
                                      entry.name
                                    )
                                  }
                                >
                                  <Download className="size-4 mr-2" />
                                  Import Table
                                </ContextMenuItem>
                              )}
                            </ContextMenuContent>
                          </ContextMenu>
                          {hasColumns && tableExpanded && (
                            <div className="ml-3 border-l border-border/50 flex flex-col py-0.5">
                              {entry.columns.map((col) => (
                                <ColumnTreeRow key={col.name} column={col} />
                              ))}
                            </div>
                          )}
                        </div>
                      );
                    })}
                </div>
              );
            })
          )}
          {!useMultiDbUi && catalog.length === 0 && !catalogIsLoading && (
            <p className="px-3 py-2 text-[11px] text-muted-foreground italic">No tables found</p>
          )}
        </div>
      )}
    </div>
  );
});
