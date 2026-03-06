import { useState, useCallback } from "react";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { useEffect } from "react";
import { Sidebar } from "./Sidebar";
import { DatasetViewer } from "../dataset/DatasetViewer";
import { QueryEditor } from "../query/QueryEditor";
import { ResultsTable } from "../query/ResultsTable";
import { ConnectionManager } from "../cloud/ConnectionManager";
import { ExportModal } from "../export/ExportModal";
import { useDataset } from "../../hooks/useDataset";
import { useQueryEngine } from "../../hooks/useQuery";
import type { DatasetInfo } from "../../lib/types";

type View = "dataset" | "query";

export function AppLayout() {
  const dataset = useDataset();
  const queryEngine = useQueryEngine();
  const [view, setView] = useState<View>("dataset");
  const [showConnections, setShowConnections] = useState(false);
  const [showExport, setShowExport] = useState(false);
  const [querySql, setQuerySql] = useState("");

  useEffect(() => {
    const currentWindow = getCurrentWindow();
    const unlisten = currentWindow.onDragDropEvent(async (event) => {
      if (event.payload.type === "drop") {
        for (const path of event.payload.paths) {
          await dataset.openFile(path);
        }
      }
    });
    return () => {
      unlisten.then((fn) => fn());
    };
  }, [dataset.openFile]);

  const handleQuerySelect = useCallback((sql: string) => {
    setQuerySql(sql);
    setView("query");
  }, []);

  const handleRemoteDatasetOpen = useCallback(
    (ds: DatasetInfo) => {
      dataset.setDatasets((prev) => {
        if (prev.find((d) => d.id === ds.id)) return prev;
        return [...prev, ds];
      });
      dataset.selectDataset(ds);
      setShowConnections(false);
    },
    [dataset]
  );

  const exportQuery = dataset.activeDataset
    ? `SELECT * FROM ${dataset.activeDataset.duckdb_ref}`
    : "";

  return (
    <div className="flex h-screen bg-zinc-950 text-zinc-200">
      <Sidebar
        datasets={dataset.datasets}
        activeDatasetId={dataset.activeDataset?.id ?? null}
        queryHistory={queryEngine.history}
        onFileOpen={dataset.openFile}
        onDatasetSelect={(ds) => {
          dataset.selectDataset(ds);
          setView("dataset");
        }}
        onQuerySelect={handleQuerySelect}
        onOpenConnections={() => setShowConnections(true)}
      />

      <main className="flex-1 flex flex-col overflow-hidden">
        {/* Tab Bar */}
        <div className="flex items-center border-b border-zinc-700 bg-zinc-900 px-4 shrink-0">
          <button
            onClick={() => setView("dataset")}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              view === "dataset"
                ? "border-blue-500 text-blue-400"
                : "border-transparent text-zinc-400 hover:text-zinc-200"
            }`}
          >
            Data
          </button>
          <button
            onClick={() => setView("query")}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              view === "query"
                ? "border-blue-500 text-blue-400"
                : "border-transparent text-zinc-400 hover:text-zinc-200"
            }`}
          >
            Query
          </button>

          <div className="flex-1" />

          {dataset.activeDataset && (
            <button
              onClick={() => setShowExport(true)}
              className="text-xs px-3 py-1.5 bg-zinc-700 hover:bg-zinc-600 text-zinc-200 rounded transition-colors"
            >
              Export
            </button>
          )}
        </div>

        {/* Content */}
        <div className="flex-1 overflow-hidden">
          {view === "dataset" ? (
            dataset.activeDataset ? (
              <DatasetViewer
                dataset={dataset.activeDataset}
                previewData={dataset.previewData}
                loading={dataset.loading}
                error={dataset.error}
                pagination={dataset.pagination}
                onPageChange={dataset.changePage}
                onPageSizeChange={dataset.changePageSize}
              />
            ) : (
              <EmptyState onOpenFile={dataset.openFile} />
            )
          ) : (
            <div className="flex flex-col h-full">
              <div className="h-1/3 min-h-[200px] border-b border-zinc-700">
                <QueryEditor
                  initialSql={querySql}
                  loading={queryEngine.loading}
                  onExecute={queryEngine.executeQuery}
                />
              </div>
              <div className="flex-1 overflow-hidden">
                <ResultsTable
                  result={queryEngine.result}
                  loading={queryEngine.loading}
                  error={queryEngine.error}
                  executionTimeMs={queryEngine.executionTimeMs}
                />
              </div>
            </div>
          )}
        </div>
      </main>

      {showConnections && (
        <ConnectionManager
          onClose={() => setShowConnections(false)}
          onDatasetOpen={handleRemoteDatasetOpen}
        />
      )}

      {showExport && (
        <ExportModal
          defaultQuery={exportQuery}
          onClose={() => setShowExport(false)}
        />
      )}
    </div>
  );
}

function EmptyState({ onOpenFile }: { onOpenFile: (path: string) => void }) {
  const handleClick = async () => {
    const { open } = await import("@tauri-apps/plugin-dialog");
    const result = await open({
      multiple: false,
      filters: [
        {
          name: "Data Files",
          extensions: ["csv", "tsv", "json", "jsonl", "parquet", "arrow", "ipc"],
        },
      ],
    });
    if (result) {
      onOpenFile(result);
    }
  };

  return (
    <div className="flex flex-col items-center justify-center h-full text-zinc-500">
      <div className="text-6xl mb-6 opacity-30">&#128202;</div>
      <h2 className="text-xl font-medium text-zinc-300 mb-2">
        No dataset loaded
      </h2>
      <p className="text-sm mb-6 text-zinc-500">
        Open a file or drag and drop to get started
      </p>
      <button
        onClick={handleClick}
        className="px-5 py-2.5 bg-blue-600 hover:bg-blue-500 text-white rounded-lg text-sm font-medium transition-colors"
      >
        Open File
      </button>
    </div>
  );
}
