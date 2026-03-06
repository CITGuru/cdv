import { useState, useCallback, useEffect } from "react";
import Editor from "@monaco-editor/react";

interface QueryEditorProps {
  initialSql?: string;
  loading: boolean;
  onExecute: (sql: string) => void;
}

export function QueryEditor({ initialSql, loading, onExecute }: QueryEditorProps) {
  const [sql, setSql] = useState(initialSql ?? "SELECT * FROM ");

  useEffect(() => {
    if (initialSql) {
      setSql(initialSql);
    }
  }, [initialSql]);

  const handleExecute = useCallback(() => {
    const trimmed = sql.trim();
    if (trimmed) {
      onExecute(trimmed);
    }
  }, [sql, onExecute]);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-4 py-2 border-b border-zinc-700 bg-zinc-900 shrink-0">
        <span className="text-xs font-semibold text-zinc-400 uppercase tracking-wider">
          SQL Query
        </span>
        <div className="flex items-center gap-2">
          <span className="text-xs text-zinc-600">Ctrl+Enter to run</span>
          <button
            onClick={handleExecute}
            disabled={loading || !sql.trim()}
            className="px-4 py-1.5 bg-blue-600 hover:bg-blue-500 disabled:bg-zinc-700 disabled:text-zinc-500 text-white text-xs font-medium rounded transition-colors"
          >
            {loading ? "Running..." : "Run"}
          </button>
        </div>
      </div>
      <div className="flex-1">
        <Editor
          language="sql"
          value={sql}
          onChange={(val) => setSql(val ?? "")}
          theme="vs-dark"
          options={{
            minimap: { enabled: false },
            fontSize: 13,
            lineNumbers: "on",
            scrollBeyondLastLine: false,
            wordWrap: "on",
            tabSize: 2,
            padding: { top: 8 },
          }}
          onMount={(editor) => {
            editor.addAction({
              id: "run-query",
              label: "Run Query",
              keybindings: [
                // Monaco.KeyMod.CtrlCmd | Monaco.KeyCode.Enter
                2048 | 3,
              ],
              run: () => {
                const text = editor.getValue().trim();
                if (text) onExecute(text);
              },
            });
          }}
        />
      </div>
    </div>
  );
}
