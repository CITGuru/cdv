import { useState } from "react";
import { save } from "@tauri-apps/plugin-dialog";
import { exportData } from "../../lib/ipc";

interface ExportModalProps {
  defaultQuery: string;
  onClose: () => void;
}

export function ExportModal({ defaultQuery, onClose }: ExportModalProps) {
  const [query, setQuery] = useState(defaultQuery);
  const [format, setFormat] = useState<"csv" | "parquet" | "json">("csv");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const handleExport = async () => {
    const ext = format === "parquet" ? "parquet" : format === "json" ? "json" : "csv";
    const outputPath = await save({
      defaultPath: `export.${ext}`,
      filters: [{ name: format.toUpperCase(), extensions: [ext] }],
    });

    if (!outputPath) return;

    setLoading(true);
    setError(null);
    setSuccess(false);

    try {
      await exportData(query, format, outputPath);
      setSuccess(true);
      setTimeout(() => onClose(), 1500);
    } catch (e) {
      setError(typeof e === "string" ? e : String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div className="bg-zinc-900 border border-zinc-700 rounded-lg shadow-2xl w-full max-w-md">
        <div className="flex items-center justify-between px-5 py-4 border-b border-zinc-700">
          <h2 className="text-sm font-semibold text-zinc-200">Export Data</h2>
          <button
            onClick={onClose}
            className="text-zinc-500 hover:text-zinc-300 text-lg"
          >
            ✕
          </button>
        </div>

        <div className="p-5 space-y-4">
          {error && (
            <div className="px-3 py-2 bg-red-900/30 border border-red-800 rounded text-sm text-red-300">
              {error}
            </div>
          )}

          {success && (
            <div className="px-3 py-2 bg-green-900/30 border border-green-800 rounded text-sm text-green-300">
              Export complete!
            </div>
          )}

          <div>
            <label className="block text-xs text-zinc-400 mb-1">Format</label>
            <div className="flex gap-2">
              {(["csv", "parquet", "json"] as const).map((f) => (
                <button
                  key={f}
                  onClick={() => setFormat(f)}
                  className={`flex-1 px-3 py-2 rounded text-sm font-medium transition-colors ${
                    format === f
                      ? "bg-blue-600 text-white"
                      : "bg-zinc-800 text-zinc-400 hover:bg-zinc-700"
                  }`}
                >
                  {f.toUpperCase()}
                </button>
              ))}
            </div>
          </div>

          <div>
            <label className="block text-xs text-zinc-400 mb-1">Query</label>
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              rows={4}
              className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded text-sm text-zinc-200 font-mono resize-none focus:outline-none focus:border-blue-500"
            />
          </div>

          <button
            onClick={handleExport}
            disabled={loading || !query.trim()}
            className="w-full px-4 py-2.5 bg-blue-600 hover:bg-blue-500 disabled:bg-zinc-700 disabled:text-zinc-500 text-white text-sm font-medium rounded transition-colors"
          >
            {loading ? "Exporting..." : "Export"}
          </button>
        </div>
      </div>
    </div>
  );
}
