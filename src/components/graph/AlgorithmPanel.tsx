import { useState, useMemo } from "react";
import { Loader2, FlaskConical } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { PropertyGraphInfo, GraphAlgorithm } from "@/lib/types";
import { GRAPH_ALGORITHMS } from "@/lib/types";
import type { ParsedError } from "@/lib/errors";
import { extractError } from "@/lib/errors";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";
import { DataTable } from "@/components/dataset/DataTable";
import { runGraphAlgorithm } from "@/lib/ipc";
import { decodeArrowIPC } from "@/lib/arrow";

interface AlgorithmPanelProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  graphName: string;
  propertyGraphs: PropertyGraphInfo[];
}

export function AlgorithmPanel({
  open,
  onOpenChange,
  graphName: initialGraphName,
  propertyGraphs,
}: AlgorithmPanelProps) {
  const [selectedGraph, setSelectedGraph] = useState(initialGraphName);
  const [algorithm, setAlgorithm] = useState<GraphAlgorithm>("pagerank");
  const [vertexLabel, setVertexLabel] = useState("");
  const [edgeLabel, setEdgeLabel] = useState("");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<ParsedError | null>(null);
  const [result, setResult] = useState<{
    columns: string[];
    rows: Record<string, unknown>[];
  } | null>(null);

  const currentGraph = useMemo(
    () => propertyGraphs.find((g) => g.name === selectedGraph),
    [propertyGraphs, selectedGraph]
  );

  const vertexOptions = currentGraph?.vertex_tables ?? [];
  const edgeOptions = currentGraph?.edge_tables ?? [];

  const canRun =
    selectedGraph &&
    algorithm &&
    vertexLabel &&
    edgeLabel;

  async function handleRun() {
    if (!canRun) return;
    setRunning(true);
    setError(null);
    setResult(null);
    try {
      const ipcBytes = await runGraphAlgorithm(
        selectedGraph,
        algorithm,
        vertexLabel,
        edgeLabel
      );
      if (ipcBytes.length === 0) {
        setResult({ columns: [], rows: [] });
      } else {
        const decoded = decodeArrowIPC(ipcBytes);
        setResult(decoded);
      }
    } catch (e) {
      setError(extractError(e));
    } finally {
      setRunning(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl max-h-[85vh] flex flex-col">
        <DialogTitle className="flex items-center gap-2">
          <FlaskConical className="size-4 text-cyan-400" />
          Graph Algorithm
        </DialogTitle>

        <Separator />

        <div className="grid grid-cols-2 gap-3">
          <div className="space-y-1">
            <Label className="text-xs">Property Graph</Label>
            <Select value={selectedGraph} onValueChange={(v) => {
              setSelectedGraph(v);
              setVertexLabel("");
              setEdgeLabel("");
              setResult(null);
            }}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Select graph..." />
              </SelectTrigger>
              <SelectContent>
                {propertyGraphs.map((g) => (
                  <SelectItem key={g.name} value={g.name}>
                    {g.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-1">
            <Label className="text-xs">Algorithm</Label>
            <Select value={algorithm} onValueChange={(v) => setAlgorithm(v as GraphAlgorithm)}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {GRAPH_ALGORITHMS.map((a) => (
                  <SelectItem key={a.value} value={a.value}>
                    {a.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-1">
            <Label className="text-xs">Vertex Label</Label>
            <Select value={vertexLabel} onValueChange={setVertexLabel} disabled={vertexOptions.length === 0}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Select vertex table..." />
              </SelectTrigger>
              <SelectContent>
                {vertexOptions.map((v) => (
                  <SelectItem key={v} value={v}>
                    {v}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-1">
            <Label className="text-xs">Edge Label</Label>
            <Select value={edgeLabel} onValueChange={setEdgeLabel} disabled={edgeOptions.length === 0}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Select edge table..." />
              </SelectTrigger>
              <SelectContent>
                {edgeOptions.map((e) => (
                  <SelectItem key={e} value={e}>
                    {e}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Button size="sm" onClick={handleRun} disabled={!canRun || running}>
            {running && <Loader2 className="size-3 mr-1 animate-spin" />}
            Run Algorithm
          </Button>
          {result && (
            <Badge variant="outline" className="text-[10px]">
              {result.rows.length} rows
            </Badge>
          )}
        </div>

        {error && (
          <ErrorDisplay error={error} compact />
        )}

        {result && result.rows.length > 0 && (
          <div className="flex-1 min-h-0 overflow-hidden border rounded-lg">
            <DataTable columns={result.columns} rows={result.rows} />
          </div>
        )}

        {result && result.rows.length === 0 && !error && (
          <div className="text-center py-6 text-muted-foreground text-sm">
            Algorithm returned no results.
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}