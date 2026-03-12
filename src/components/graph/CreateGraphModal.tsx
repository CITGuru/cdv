import { useState, useMemo } from "react";
import {
  Loader2,
  ArrowLeft,
  Plus,
  Trash2,
  Network,
  CheckCircle2,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { DataSource, VertexTableDef, EdgeTableDef } from "@/lib/types";
import type { ParsedError } from "@/lib/errors";
import { extractError } from "@/lib/errors";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";
import { createPropertyGraph } from "@/lib/ipc";

type Step = "name" | "vertices" | "edges" | "review";
const STEPS: Step[] = ["name", "vertices", "edges", "review"];

interface VertexEntry {
  id: number;
  table_name: string;
  key_column: string;
  label: string;
}

interface EdgeEntry {
  id: number;
  table_name: string;
  source_key: string;
  source_vertex_table: string;
  source_vertex_key: string;
  destination_key: string;
  destination_vertex_table: string;
  destination_vertex_key: string;
  label: string;
}

interface CreateGraphModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  dataSources: DataSource[];
  onCreated: () => void;
}

function quoteIdent(id: string): string {
  if (id.includes(".") || id.includes('"')) return id;
  return `"${id}"`;
}

function buildDdlPreview(
  name: string,
  vertices: VertexEntry[],
  edges: EdgeEntry[]
): string {
  let ddl = `CREATE PROPERTY GRAPH ${quoteIdent(name)}`;
  ddl += "\nVERTEX TABLES (\n";
  ddl += vertices
    .map((v) => {
      let s = `    ${quoteIdent(v.table_name)}`;
      if (v.label) s += ` LABEL ${quoteIdent(v.label)}`;
      return s;
    })
    .join(",\n");
  ddl += "\n)";

  if (edges.length > 0) {
    ddl += "\nEDGE TABLES (\n";
    ddl += edges
      .map((e) => {
        let s = `    ${quoteIdent(e.table_name)}`;
        s += `\n        SOURCE KEY (${quoteIdent(e.source_key)}) REFERENCES ${quoteIdent(e.source_vertex_table)} (${quoteIdent(e.source_vertex_key)})`;
        s += `\n        DESTINATION KEY (${quoteIdent(e.destination_key)}) REFERENCES ${quoteIdent(e.destination_vertex_table)} (${quoteIdent(e.destination_vertex_key)})`;
        if (e.label) s += ` LABEL ${quoteIdent(e.label)}`;
        return s;
      })
      .join(",\n");
    ddl += "\n)";
  }

  ddl += ";";
  return ddl;
}

export function CreateGraphModal({
  open,
  onOpenChange,
  dataSources,
  onCreated,
}: CreateGraphModalProps) {
  const [step, setStep] = useState<Step>("name");
  const [graphName, setGraphName] = useState("");
  const [vertices, setVertices] = useState<VertexEntry[]>([]);
  const [edges, setEdges] = useState<EdgeEntry[]>([]);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<ParsedError | null>(null);
  const [nextId, setNextId] = useState(1);

  const tableOptions = useMemo(() => {
    return dataSources
      .filter((ds) => ds.view_name || ds.qualified_name)
      .map((ds) => ({
        name: ds.view_name ?? ds.qualified_name,
        columns: ds.schema.map((c) => c.name),
      }));
  }, [dataSources]);

  const vertexTableNames = useMemo(
    () => vertices.map((v) => v.table_name),
    [vertices]
  );

  const stepIdx = STEPS.indexOf(step);

  function reset() {
    setStep("name");
    setGraphName("");
    setVertices([]);
    setEdges([]);
    setCreating(false);
    setError(null);
    setNextId(1);
  }

  function handleClose(val: boolean) {
    if (!val) reset();
    onOpenChange(val);
  }

  function addVertex() {
    setVertices((prev) => [
      ...prev,
      { id: nextId, table_name: "", key_column: "", label: "" },
    ]);
    setNextId((n) => n + 1);
  }

  function updateVertex(id: number, patch: Partial<VertexEntry>) {
    setVertices((prev) =>
      prev.map((v) => (v.id === id ? { ...v, ...patch } : v))
    );
  }

  function removeVertex(id: number) {
    setVertices((prev) => prev.filter((v) => v.id !== id));
  }

  function addEdge() {
    setEdges((prev) => [
      ...prev,
      {
        id: nextId,
        table_name: "",
        source_key: "",
        source_vertex_table: "",
        source_vertex_key: "",
        destination_key: "",
        destination_vertex_table: "",
        destination_vertex_key: "",
        label: "",
      },
    ]);
    setNextId((n) => n + 1);
  }

  function updateEdge(id: number, patch: Partial<EdgeEntry>) {
    setEdges((prev) =>
      prev.map((e) => (e.id === id ? { ...e, ...patch } : e))
    );
  }

  function removeEdge(id: number) {
    setEdges((prev) => prev.filter((e) => e.id !== id));
  }

  function columnsFor(tableName: string): string[] {
    return tableOptions.find((t) => t.name === tableName)?.columns ?? [];
  }

  function canAdvance(): boolean {
    switch (step) {
      case "name":
        return graphName.trim().length > 0;
      case "vertices":
        return (
          vertices.length > 0 &&
          vertices.every((v) => v.table_name.length > 0)
        );
      case "edges":
        return edges.every(
          (e) =>
            e.table_name.length > 0 &&
            e.source_key.length > 0 &&
            e.source_vertex_table.length > 0 &&
            e.source_vertex_key.length > 0 &&
            e.destination_key.length > 0 &&
            e.destination_vertex_table.length > 0 &&
            e.destination_vertex_key.length > 0
        );
      default:
        return true;
    }
  }

  function goNext() {
    setError(null);
    const idx = STEPS.indexOf(step);
    if (idx < STEPS.length - 1) setStep(STEPS[idx + 1]);
  }

  function goBack() {
    setError(null);
    const idx = STEPS.indexOf(step);
    if (idx > 0) setStep(STEPS[idx - 1]);
  }

  async function handleCreate() {
    setCreating(true);
    setError(null);
    try {
      const vDefs: VertexTableDef[] = vertices.map((v) => ({
        table_name: v.table_name,
        key_column: v.key_column || null,
        label: v.label || null,
      }));
      const eDefs: EdgeTableDef[] = edges.map((e) => ({
        table_name: e.table_name,
        source_key: e.source_key,
        source_vertex_table: e.source_vertex_table,
        source_vertex_key: e.source_vertex_key,
        destination_key: e.destination_key,
        destination_vertex_table: e.destination_vertex_table,
        destination_vertex_key: e.destination_vertex_key,
        label: e.label || null,
      }));
      await createPropertyGraph(graphName.trim(), vDefs, eDefs);
      onCreated();
      handleClose(false);
    } catch (e) {
      setError(extractError(e));
    } finally {
      setCreating(false);
    }
  }

  const ddlPreview = useMemo(
    () => buildDdlPreview(graphName, vertices, edges),
    [graphName, vertices, edges]
  );

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-xl max-h-[85vh] flex flex-col">
        <DialogTitle className="flex items-center gap-2">
          <Network className="size-4 text-cyan-400" />
          Create Property Graph
          <Badge variant="outline" className="ml-auto text-[10px]">
            Step {stepIdx + 1} of {STEPS.length}
          </Badge>
        </DialogTitle>

        <Separator />

        <ScrollArea className="flex-1 min-h-0 pr-2">
          {step === "name" && (
            <div className="space-y-4 py-2">
              <div className="space-y-2">
                <Label>Graph Name</Label>
                <Input
                  placeholder="e.g. social_network"
                  value={graphName}
                  onChange={(e) => setGraphName(e.target.value)}
                  autoFocus
                />
                <p className="text-xs text-muted-foreground">
                  A unique name for the property graph. This will be used in
                  GRAPH_TABLE queries.
                </p>
              </div>
            </div>
          )}

          {step === "vertices" && (
            <div className="space-y-4 py-2">
              <div className="flex items-center justify-between">
                <Label>Vertex Tables</Label>
                <Button size="sm" variant="outline" onClick={addVertex}>
                  <Plus className="size-3 mr-1" />
                  Add Vertex Table
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                Select which tables represent nodes in the graph. Each vertex
                table must be an existing data source or view.
              </p>
              {vertices.length === 0 && (
                <div className="text-center py-8 text-muted-foreground text-sm">
                  No vertex tables added yet. Click &quot;Add Vertex Table&quot;
                  to start.
                </div>
              )}
              {vertices.map((v) => (
                <div
                  key={v.id}
                  className="border rounded-lg p-3 space-y-3 bg-muted/30"
                >
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-medium text-muted-foreground">
                      Vertex Table
                    </span>
                    <Button
                      size="icon-xs"
                      variant="ghost"
                      onClick={() => removeVertex(v.id)}
                    >
                      <Trash2 className="size-3 text-destructive" />
                    </Button>
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div className="space-y-1">
                      <Label className="text-xs">Table / View</Label>
                      <Select
                        value={v.table_name}
                        onValueChange={(val) =>
                          updateVertex(v.id, {
                            table_name: val,
                            key_column: "",
                          })
                        }
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder="Select table..." />
                        </SelectTrigger>
                        <SelectContent>
                          {tableOptions.map((t) => (
                            <SelectItem key={t.name} value={t.name}>
                              {t.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1">
                      <Label className="text-xs">Key Column (optional)</Label>
                      <Select
                        value={v.key_column}
                        onValueChange={(val) =>
                          updateVertex(v.id, { key_column: val })
                        }
                        disabled={!v.table_name}
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder="Auto (rowid)" />
                        </SelectTrigger>
                        <SelectContent>
                          {columnsFor(v.table_name).map((c) => (
                            <SelectItem key={c} value={c}>
                              {c}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">Label (optional)</Label>
                    <Input
                      className="h-8 text-xs"
                      placeholder="Optional label override"
                      value={v.label}
                      onChange={(e) =>
                        updateVertex(v.id, { label: e.target.value })
                      }
                    />
                  </div>
                </div>
              ))}
            </div>
          )}

          {step === "edges" && (
            <div className="space-y-4 py-2">
              <div className="flex items-center justify-between">
                <Label>Edge Tables</Label>
                <Button size="sm" variant="outline" onClick={addEdge}>
                  <Plus className="size-3 mr-1" />
                  Add Edge Table
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                Select tables that represent relationships between vertex
                tables. Map source and destination keys to the appropriate vertex
                table columns.
              </p>
              {edges.length === 0 && (
                <div className="text-center py-8 text-muted-foreground text-sm">
                  No edge tables yet. You can skip this step if your graph has
                  no edges.
                </div>
              )}
              {edges.map((e) => (
                <div
                  key={e.id}
                  className="border rounded-lg p-3 space-y-3 bg-muted/30"
                >
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-medium text-muted-foreground">
                      Edge Table
                    </span>
                    <Button
                      size="icon-xs"
                      variant="ghost"
                      onClick={() => removeEdge(e.id)}
                    >
                      <Trash2 className="size-3 text-destructive" />
                    </Button>
                  </div>

                  <div className="space-y-1">
                    <Label className="text-xs">Table / View</Label>
                    <Select
                      value={e.table_name}
                      onValueChange={(val) =>
                        updateEdge(e.id, {
                          table_name: val,
                          source_key: "",
                          destination_key: "",
                        })
                      }
                    >
                      <SelectTrigger className="h-8 text-xs">
                        <SelectValue placeholder="Select table..." />
                      </SelectTrigger>
                      <SelectContent>
                        {tableOptions.map((t) => (
                          <SelectItem key={t.name} value={t.name}>
                            {t.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <Separator />

                  <div className="grid grid-cols-3 gap-2">
                    <div className="space-y-1">
                      <Label className="text-xs">Source Key</Label>
                      <Select
                        value={e.source_key}
                        onValueChange={(val) =>
                          updateEdge(e.id, { source_key: val })
                        }
                        disabled={!e.table_name}
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder="Column..." />
                        </SelectTrigger>
                        <SelectContent>
                          {columnsFor(e.table_name).map((c) => (
                            <SelectItem key={c} value={c}>
                              {c}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1">
                      <Label className="text-xs">References Table</Label>
                      <Select
                        value={e.source_vertex_table}
                        onValueChange={(val) =>
                          updateEdge(e.id, {
                            source_vertex_table: val,
                            source_vertex_key: "",
                          })
                        }
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder="Vertex table..." />
                        </SelectTrigger>
                        <SelectContent>
                          {vertexTableNames.map((n) => (
                            <SelectItem key={n} value={n}>
                              {n}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1">
                      <Label className="text-xs">References Column</Label>
                      <Select
                        value={e.source_vertex_key}
                        onValueChange={(val) =>
                          updateEdge(e.id, { source_vertex_key: val })
                        }
                        disabled={!e.source_vertex_table}
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder="Column..." />
                        </SelectTrigger>
                        <SelectContent>
                          {columnsFor(e.source_vertex_table).map((c) => (
                            <SelectItem key={c} value={c}>
                              {c}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  <div className="grid grid-cols-3 gap-2">
                    <div className="space-y-1">
                      <Label className="text-xs">Destination Key</Label>
                      <Select
                        value={e.destination_key}
                        onValueChange={(val) =>
                          updateEdge(e.id, { destination_key: val })
                        }
                        disabled={!e.table_name}
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder="Column..." />
                        </SelectTrigger>
                        <SelectContent>
                          {columnsFor(e.table_name).map((c) => (
                            <SelectItem key={c} value={c}>
                              {c}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1">
                      <Label className="text-xs">References Table</Label>
                      <Select
                        value={e.destination_vertex_table}
                        onValueChange={(val) =>
                          updateEdge(e.id, {
                            destination_vertex_table: val,
                            destination_vertex_key: "",
                          })
                        }
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder="Vertex table..." />
                        </SelectTrigger>
                        <SelectContent>
                          {vertexTableNames.map((n) => (
                            <SelectItem key={n} value={n}>
                              {n}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1">
                      <Label className="text-xs">References Column</Label>
                      <Select
                        value={e.destination_vertex_key}
                        onValueChange={(val) =>
                          updateEdge(e.id, { destination_vertex_key: val })
                        }
                        disabled={!e.destination_vertex_table}
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder="Column..." />
                        </SelectTrigger>
                        <SelectContent>
                          {columnsFor(e.destination_vertex_table).map((c) => (
                            <SelectItem key={c} value={c}>
                              {c}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  <div className="space-y-1">
                    <Label className="text-xs">Label (optional)</Label>
                    <Input
                      className="h-8 text-xs"
                      placeholder="Optional label override"
                      value={e.label}
                      onChange={(ev) =>
                        updateEdge(e.id, { label: ev.target.value })
                      }
                    />
                  </div>
                </div>
              ))}
            </div>
          )}

          {step === "review" && (
            <div className="space-y-4 py-2">
              <div className="flex items-center gap-2 text-sm">
                <CheckCircle2 className="size-4 text-green-500" />
                <span className="font-medium">Review &amp; Create</span>
              </div>
              <div className="grid grid-cols-3 gap-3 text-xs">
                <div>
                  <span className="text-muted-foreground">Graph Name</span>
                  <p className="font-medium">{graphName}</p>
                </div>
                <div>
                  <span className="text-muted-foreground">Vertex Tables</span>
                  <p className="font-medium">{vertices.length}</p>
                </div>
                <div>
                  <span className="text-muted-foreground">Edge Tables</span>
                  <p className="font-medium">{edges.length}</p>
                </div>
              </div>
              <div className="space-y-1">
                <Label className="text-xs">Generated DDL</Label>
                <pre className="text-[11px] font-mono bg-muted rounded-lg p-3 whitespace-pre-wrap break-words border overflow-auto max-h-48">
                  {ddlPreview}
                </pre>
              </div>
            </div>
          )}
        </ScrollArea>

        {error && (
          <div className="mt-2">
            <ErrorDisplay error={error} compact />
          </div>
        )}

        <Separator />

        <div className="flex items-center justify-between">
          <div>
            {stepIdx > 0 && (
              <Button
                variant="ghost"
                size="sm"
                onClick={goBack}
                disabled={creating}
              >
                <ArrowLeft className="size-3 mr-1" />
                Back
              </Button>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => handleClose(false)}
              disabled={creating}
            >
              Cancel
            </Button>
            {step === "review" ? (
              <Button size="sm" onClick={handleCreate} disabled={creating}>
                {creating && <Loader2 className="size-3 mr-1 animate-spin" />}
                Create Graph
              </Button>
            ) : (
              <Button
                size="sm"
                onClick={goNext}
                disabled={!canAdvance()}
              >
                Next
              </Button>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
