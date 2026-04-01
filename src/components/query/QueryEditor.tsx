import { useState, useCallback, useEffect, useRef, useMemo } from "react";
import Editor, { type Monaco } from "@monaco-editor/react";
import type { editor as monacoEditor, IDisposable, Position } from "monaco-editor";
import { Play, Loader2, Keyboard } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import type { DataSource, Connector, CatalogEntry } from "@/lib/types";

interface QueryEditorProps {
  initialSql?: string;
  loading: boolean;
  onExecute: (sql: string) => void;
  onSqlChange?: (sql: string) => void;
  dataSources?: DataSource[];
  connectors?: Connector[];
  catalogs?: Record<string, CatalogEntry[]>;
}

function quoteIfNeeded(name: string): string {
  return /^[a-z_][a-z0-9_]*$/.test(name) ? name : `"${name}"`;
}

const FROM_JOIN_RE = /\b(FROM|JOIN)\s+("?)(\w*)$/im;
const DOT_RE = /\b"?(\w+)"?\.\s*("?)(\w*)$/im;
const COLUMN_CONTEXT_RE = /(?:\b(?:SELECT|WHERE|AND|OR|ON|SET|HAVING|BY)\s+|,\s*)\w*$/im;
const REFERENCED_TABLES_RE = /\b(?:FROM|JOIN)\s+"?(\w+)"?/gim;

function resolveSqlToRun(
  editor: monacoEditor.IStandaloneCodeEditor | null,
  fallbackDocumentSql: string
): { sqlToRun: string; runsSelection: boolean } {
  if (editor) {
    const model = editor.getModel();
    const sel = editor.getSelection();
    if (model && sel && !sel.isEmpty()) {
      const fromSelection = model.getValueInRange(sel).trim();
      if (fromSelection) return { sqlToRun: fromSelection, runsSelection: true };
    }
    return { sqlToRun: editor.getValue().trim(), runsSelection: false };
  }
  return { sqlToRun: fallbackDocumentSql.trim(), runsSelection: false };
}

export function QueryEditor({ initialSql, loading, onExecute, onSqlChange, dataSources = [], connectors: _connectors = [], catalogs: _catalogs = {} }: QueryEditorProps) {
  const [sql, setSql] = useState(initialSql ?? "SELECT * FROM ");
  const [selectionTick, setSelectionTick] = useState(0);
  const editorRef = useRef<monacoEditor.IStandaloneCodeEditor | null>(null);
  const completionDisposableRef = useRef<IDisposable | null>(null);
  const selectionDisposableRef = useRef<IDisposable | null>(null);
  const dataSourcesRef = useRef(dataSources);
  dataSourcesRef.current = dataSources;
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const { sqlToRun, runsSelection } = useMemo(
    () => resolveSqlToRun(editorRef.current, sql),
    [sql, selectionTick]
  );

  useEffect(() => {
    if (initialSql) {
      setSql(initialSql);
    }
  }, [initialSql]);

  useEffect(() => {
    return () => {
      completionDisposableRef.current?.dispose();
      selectionDisposableRef.current?.dispose();
      editorRef.current = null;
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, []);

  const handleChange = useCallback(
    (val: string | undefined) => {
      const next = val ?? "";
      setSql(next);
      if (onSqlChange) {
        if (debounceRef.current) clearTimeout(debounceRef.current);
        debounceRef.current = setTimeout(() => onSqlChange(next), 500);
      }
    },
    [onSqlChange]
  );

  const handleExecute = useCallback(() => {
    const { sqlToRun: text } = resolveSqlToRun(editorRef.current, sql);
    if (text) onExecute(text);
  }, [sql, onExecute]);

  const handleMount = useCallback(
    (editor: monacoEditor.IStandaloneCodeEditor, monaco: Monaco) => {
      editorRef.current = editor;
      selectionDisposableRef.current?.dispose();
      selectionDisposableRef.current = editor.onDidChangeCursorSelection(() => {
        setSelectionTick((t) => t + 1);
      });
      setSelectionTick((t) => t + 1);

      editor.addAction({
        id: "run-feature",
        label: "Run feature",
        keybindings: [2048 | 3],
        run: () => {
          const { sqlToRun: text } = resolveSqlToRun(editor, "");
          if (text) onExecute(text);
        },
      });

      completionDisposableRef.current?.dispose();
      completionDisposableRef.current = monaco.languages.registerCompletionItemProvider("sql", {
        triggerCharacters: [" ", ".", "\n", ","],
        provideCompletionItems(model: monacoEditor.ITextModel, position: Position) {
          const textUntilPosition = model.getValueInRange({
            startLineNumber: 1,
            startColumn: 1,
            endLineNumber: position.lineNumber,
            endColumn: position.column,
          });

          const word = model.getWordUntilPosition(position);
          const replaceRange = {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn,
          };

          const sources = dataSourcesRef.current;
          if (sources.length === 0) return { suggestions: [] };

          const dotMatch = textUntilPosition.match(DOT_RE);
          if (dotMatch) {
            const tableName = dotMatch[1];
            const ds = sources.find(
              (s) => (s.view_name ?? "").toLowerCase() === tableName.toLowerCase() ||
                     s.name.toLowerCase() === tableName.toLowerCase()
            );
            if (ds) {
              return {
                suggestions: ds.schema.map((col, i) => ({
                  label: col.name,
                  kind: monaco.languages.CompletionItemKind.Field,
                  insertText: quoteIfNeeded(col.name),
                  detail: col.data_type + (col.nullable ? " (nullable)" : ""),
                  documentation: col.key ? `Key: ${col.key}` : undefined,
                  sortText: String(i).padStart(4, "0"),
                  range: replaceRange,
                })),
              };
            }
          }

          if (FROM_JOIN_RE.test(textUntilPosition)) {
            const suggestions: any[] = sources.map((ds, i) => ({
              label: ds.view_name ?? ds.name,
              kind: monaco.languages.CompletionItemKind.Struct,
              insertText: ds.qualified_name,
              detail: ds.name,
              documentation: ds.schema.map((c) => `${c.name}: ${c.data_type}`).join("\n"),
              sortText: String(i).padStart(4, "0"),
              range: replaceRange,
            }));
            return { suggestions };
          }

          if (COLUMN_CONTEXT_RE.test(textUntilPosition)) {
            const fullText = model.getValue();
            const referencedTables: DataSource[] = [];
            let m: RegExpExecArray | null;
            const re = new RegExp(REFERENCED_TABLES_RE.source, REFERENCED_TABLES_RE.flags);
            while ((m = re.exec(fullText)) !== null) {
              const tbl = m[1];
              const ds = sources.find(
                (s) => (s.view_name ?? "").toLowerCase() === tbl.toLowerCase() ||
                       s.name.toLowerCase() === tbl.toLowerCase()
              );
              if (ds && !referencedTables.some((r) => r.id === ds.id)) {
                referencedTables.push(ds);
              }
            }
            if (referencedTables.length === 0) referencedTables.push(...sources);

            const seen = new Set<string>();
            const suggestions: any[] = [];
            const multiTable = referencedTables.length > 1;
            for (const ds of referencedTables) {
              for (let i = 0; i < ds.schema.length; i++) {
                const col = ds.schema[i];
                const dsLabel = ds.view_name ?? ds.name;
                const key = multiTable ? `${dsLabel}.${col.name}` : col.name;
                if (seen.has(key)) continue;
                seen.add(key);
                suggestions.push({
                  label: multiTable
                    ? { label: col.name, description: dsLabel }
                    : col.name,
                  kind: monaco.languages.CompletionItemKind.Field,
                  insertText: quoteIfNeeded(col.name),
                  detail: col.data_type + (col.nullable ? " (nullable)" : ""),
                  documentation: col.key ? `Key: ${col.key}` : undefined,
                  sortText: String(suggestions.length).padStart(4, "0"),
                  range: replaceRange,
                });
              }
            }
            return { suggestions };
          }

          return { suggestions: [] };
        },
      });
    },
    [onExecute]
  );

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-start justify-between gap-3 px-3 py-1.5 border-b border-border bg-card shrink-0">
        <div className="flex flex-col gap-0.5 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-xs font-medium text-muted-foreground">Feature</span>
            <Badge
              variant="outline"
              className="text-[10px] gap-1 px-1.5 py-0 h-5 text-muted-foreground"
              title="SQL Editor"
            >
              <Keyboard className="size-2.5" />
              Ctrl+Enter
            </Badge>
          </div>

        </div>
        <Button
          size="sm"
          onClick={handleExecute}
          disabled={loading || !sqlToRun}
          className="gap-1.5 text-xs shrink-0 mt-0.5"
          title={
            runsSelection
              ? "Run only the highlighted SQL"
              : "Run all SQL in the editor"
          }
        >
          {loading ? (
            <Loader2 className="size-3.5 animate-spin" />
          ) : (
            <Play className="size-3.5" />
          )}
          {loading ? "Running…" : runsSelection ? "Run selection" : "Run feature"}
        </Button>
      </div>
      <div className="flex-1">
        <Editor
          language="sql"
          value={sql}
          onChange={handleChange}
          theme="vs-dark"
          options={{
            minimap: { enabled: false },
            fontSize: 13,
            fontFamily: "'Geist Mono', 'Fira Code', 'Cascadia Code', monospace",
            lineNumbers: "on",
            scrollBeyondLastLine: false,
            wordWrap: "on",
            tabSize: 2,
            padding: { top: 12, bottom: 12 },
            renderLineHighlight: "gutter",
            quickSuggestions: true,
            suggestOnTriggerCharacters: true,
            wordBasedSuggestions: "off",
            scrollbar: {
              verticalScrollbarSize: 8,
              horizontalScrollbarSize: 8,
            },
          }}
          onMount={handleMount}
        />
      </div>
    </div>
  );
}
