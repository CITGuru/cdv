import { useState } from "react";
import type { DatasetInfo } from "../../lib/types";

interface SchemaPanelProps {
  dataset: DatasetInfo;
}

export function SchemaPanel({ dataset }: SchemaPanelProps) {
  const [collapsed, setCollapsed] = useState(true);

  return (
    <div className="border-b border-zinc-700 bg-zinc-900/50">
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="w-full flex items-center justify-between px-4 py-2.5 text-sm hover:bg-zinc-800/50 transition-colors"
      >
        <div className="flex items-center gap-3">
          <span className="text-zinc-400 text-xs">{collapsed ? "▶" : "▼"}</span>
          <span className="font-medium text-zinc-200">{dataset.name}</span>
          <span className="text-xs px-1.5 py-0.5 bg-zinc-700 text-zinc-300 rounded">
            {dataset.format}
          </span>
        </div>
        <div className="flex items-center gap-4 text-xs text-zinc-500">
          <span>{dataset.schema.length} columns</span>
          <span>{dataset.row_count?.toLocaleString() ?? "?"} rows</span>
        </div>
      </button>

      {!collapsed && (
        <div className="px-4 pb-3">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-zinc-500 border-b border-zinc-800">
                <th className="text-left py-1.5 font-medium">Column</th>
                <th className="text-left py-1.5 font-medium">Type</th>
                <th className="text-left py-1.5 font-medium">Nullable</th>
              </tr>
            </thead>
            <tbody>
              {dataset.schema.map((col) => (
                <tr key={col.name} className="border-b border-zinc-800/50">
                  <td className="py-1.5 text-zinc-200 font-mono">{col.name}</td>
                  <td className="py-1.5 text-zinc-400 font-mono">
                    {col.data_type}
                  </td>
                  <td className="py-1.5">
                    {col.nullable ? (
                      <span className="text-yellow-500">yes</span>
                    ) : (
                      <span className="text-zinc-600">no</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
