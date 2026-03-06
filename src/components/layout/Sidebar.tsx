import { open } from "@tauri-apps/plugin-dialog";
import type { DatasetInfo } from "../../lib/types";
import type { QueryHistoryEntry } from "../../hooks/useQuery";

interface SidebarProps {
  datasets: DatasetInfo[];
  activeDatasetId: string | null;
  queryHistory: QueryHistoryEntry[];
  onFileOpen: (path: string) => void;
  onDatasetSelect: (dataset: DatasetInfo) => void;
  onQuerySelect: (sql: string) => void;
  onOpenConnections: () => void;
}

export function Sidebar({
  datasets,
  activeDatasetId,
  queryHistory,
  onFileOpen,
  onDatasetSelect,
  onQuerySelect,
  onOpenConnections,
}: SidebarProps) {
  const handleOpenFile = async () => {
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
      onFileOpen(result);
    }
  };

  return (
    <aside className="w-70 bg-zinc-900 border-r border-zinc-700 flex flex-col h-full overflow-hidden">
      <div className="p-4 border-b border-zinc-700">
        <h1 className="text-sm font-bold text-zinc-100 tracking-wide uppercase">
          CDV
        </h1>
      </div>

      <div className="flex-1 overflow-y-auto">
        {/* Files Section */}
        <section className="p-3">
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider">
              Files
            </h2>
            <button
              onClick={handleOpenFile}
              className="text-xs px-2 py-1 bg-blue-600 hover:bg-blue-500 text-white rounded transition-colors"
            >
              Open
            </button>
          </div>
          {datasets.filter((d) => d.source_type === "local").length === 0 && (
            <p className="text-xs text-zinc-500 italic">No files loaded</p>
          )}
          <ul className="space-y-0.5">
            {datasets
              .filter((d) => d.source_type === "local")
              .map((ds) => (
                <li key={ds.id}>
                  <button
                    onClick={() => onDatasetSelect(ds)}
                    className={`w-full text-left px-2 py-1.5 rounded text-sm truncate transition-colors ${
                      activeDatasetId === ds.id
                        ? "bg-blue-600/20 text-blue-300"
                        : "text-zinc-300 hover:bg-zinc-800"
                    }`}
                    title={ds.path}
                  >
                    <span className="block truncate">{ds.name}</span>
                    <span className="text-xs text-zinc-500">
                      {ds.format} · {ds.row_count?.toLocaleString() ?? "?"} rows
                    </span>
                  </button>
                </li>
              ))}
          </ul>
        </section>

        {/* Connections Section */}
        <section className="p-3 border-t border-zinc-800">
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider">
              Connections
            </h2>
            <button
              onClick={onOpenConnections}
              className="text-xs px-2 py-1 bg-zinc-700 hover:bg-zinc-600 text-zinc-200 rounded transition-colors"
            >
              Add
            </button>
          </div>
          {datasets.filter((d) => d.source_type === "s3").length === 0 && (
            <p className="text-xs text-zinc-500 italic">No connections</p>
          )}
          <ul className="space-y-0.5">
            {datasets
              .filter((d) => d.source_type === "s3")
              .map((ds) => (
                <li key={ds.id}>
                  <button
                    onClick={() => onDatasetSelect(ds)}
                    className={`w-full text-left px-2 py-1.5 rounded text-sm truncate transition-colors ${
                      activeDatasetId === ds.id
                        ? "bg-blue-600/20 text-blue-300"
                        : "text-zinc-300 hover:bg-zinc-800"
                    }`}
                    title={ds.path}
                  >
                    <span className="block truncate">{ds.name}</span>
                    <span className="text-xs text-zinc-500">
                      {ds.format} · {ds.row_count?.toLocaleString() ?? "?"} rows
                    </span>
                  </button>
                </li>
              ))}
          </ul>
        </section>

        {/* Query History Section */}
        <section className="p-3 border-t border-zinc-800">
          <h2 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider mb-2">
            Queries
          </h2>
          {queryHistory.length === 0 && (
            <p className="text-xs text-zinc-500 italic">No queries yet</p>
          )}
          <ul className="space-y-0.5">
            {queryHistory.slice(0, 10).map((entry, i) => (
              <li key={i}>
                <button
                  onClick={() => onQuerySelect(entry.sql)}
                  className="w-full text-left px-2 py-1.5 rounded text-xs text-zinc-400 hover:bg-zinc-800 transition-colors truncate"
                  title={entry.sql}
                >
                  <span
                    className={`inline-block w-1.5 h-1.5 rounded-full mr-1.5 ${
                      entry.status === "success" ? "bg-green-500" : "bg-red-500"
                    }`}
                  />
                  <span className="truncate">{entry.sql}</span>
                </button>
              </li>
            ))}
          </ul>
        </section>
      </div>
    </aside>
  );
}
