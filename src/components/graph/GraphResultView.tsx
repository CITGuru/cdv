import { useState, useMemo, useCallback, useRef, useEffect } from "react";
import ForceGraph2D from "react-force-graph-2d";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface GraphResultViewProps {
  columns: string[];
  rows: Record<string, unknown>[];
}

interface GraphNode {
  id: string;
  label: string;
}

interface GraphLink {
  source: string;
  target: string;
  label?: string;
}

const NONE = "__none__";

const SOURCE_PATTERNS = [
  "source",
  "src",
  "from",
  "person1",
  "start",
  "source_id",
  "from_id",
  "src_id",
];

const TARGET_PATTERNS = [
  "target",
  "dst",
  "to",
  "person2",
  "end",
  "destination",
  "target_id",
  "to_id",
  "dst_id",
  "dest",
];

const LABEL_PATTERNS = ["label", "name", "title", "type"];

function autoDetect(
  columns: string[],
  patterns: string[]
): string {
  const lower = columns.map((c) => c.toLowerCase());
  for (const p of patterns) {
    const idx = lower.indexOf(p);
    if (idx >= 0) return columns[idx];
  }
  for (const p of patterns) {
    const idx = lower.findIndex((c) => c.includes(p));
    if (idx >= 0) return columns[idx];
  }
  return NONE;
}

export function GraphResultView({ columns, rows }: GraphResultViewProps) {
  const [sourceCol, setSourceCol] = useState(() =>
    autoDetect(columns, SOURCE_PATTERNS)
  );
  const [targetCol, setTargetCol] = useState(() =>
    autoDetect(columns, TARGET_PATTERNS)
  );
  const [nodeLabelCol, setNodeLabelCol] = useState(() =>
    autoDetect(columns, LABEL_PATTERNS)
  );
  const [edgeLabelCol, setEdgeLabelCol] = useState(NONE);

  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 600, height: 400 });

  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        setDimensions({ width: Math.floor(width), height: Math.floor(height) });
      }
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  const graphData = useMemo(() => {
    if (sourceCol === NONE || targetCol === NONE) {
      return { nodes: [] as GraphNode[], links: [] as GraphLink[] };
    }

    const nodeSet = new Map<string, GraphNode>();
    const links: GraphLink[] = [];

    for (const row of rows) {
      const src = String(row[sourceCol] ?? "");
      const tgt = String(row[targetCol] ?? "");
      if (!src || !tgt) continue;

      if (!nodeSet.has(src)) {
        const label =
          nodeLabelCol !== NONE && row[nodeLabelCol] != null
            ? String(row[nodeLabelCol])
            : src;
        nodeSet.set(src, { id: src, label });
      }
      if (!nodeSet.has(tgt)) {
        nodeSet.set(tgt, { id: tgt, label: tgt });
      }

      links.push({
        source: src,
        target: tgt,
        label:
          edgeLabelCol !== NONE && row[edgeLabelCol] != null
            ? String(row[edgeLabelCol])
            : undefined,
      });
    }

    return {
      nodes: Array.from(nodeSet.values()),
      links,
    };
  }, [rows, sourceCol, targetCol, nodeLabelCol, edgeLabelCol]);

  const nodeCanvasObject = useCallback(
    (node: GraphNode & { x?: number; y?: number }, ctx: CanvasRenderingContext2D, globalScale: number) => {
      const label = node.label || node.id;
      const fontSize = 11 / globalScale;
      ctx.font = `${fontSize}px sans-serif`;
      const textWidth = ctx.measureText(label).width;
      const padding = 3 / globalScale;
      const r = Math.max(textWidth / 2 + padding, 8 / globalScale);

      const x = node.x ?? 0;
      const y = node.y ?? 0;

      ctx.beginPath();
      ctx.arc(x, y, r, 0, 2 * Math.PI, false);
      ctx.fillStyle = "rgba(56, 189, 248, 0.85)";
      ctx.fill();
      ctx.strokeStyle = "rgba(56, 189, 248, 0.4)";
      ctx.lineWidth = 1.5 / globalScale;
      ctx.stroke();

      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = "#0f172a";
      ctx.fillText(label, x, y);
    },
    []
  );

  const hasMapping = sourceCol !== NONE && targetCol !== NONE;

  return (
    <div className="flex flex-col h-full">
      <div className="shrink-0 flex items-center gap-4 px-3 py-2 border-b border-border bg-card flex-wrap">
        <div className="flex items-center gap-1.5">
          <Label className="text-[10px] text-muted-foreground">Source</Label>
          <Select value={sourceCol} onValueChange={setSourceCol}>
            <SelectTrigger className="h-7 text-xs w-32">
              <SelectValue placeholder="Select..." />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={NONE}>-- none --</SelectItem>
              {columns.map((c) => (
                <SelectItem key={c} value={c}>
                  {c}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex items-center gap-1.5">
          <Label className="text-[10px] text-muted-foreground">Target</Label>
          <Select value={targetCol} onValueChange={setTargetCol}>
            <SelectTrigger className="h-7 text-xs w-32">
              <SelectValue placeholder="Select..." />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={NONE}>-- none --</SelectItem>
              {columns.map((c) => (
                <SelectItem key={c} value={c}>
                  {c}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex items-center gap-1.5">
          <Label className="text-[10px] text-muted-foreground">Node Label</Label>
          <Select value={nodeLabelCol} onValueChange={setNodeLabelCol}>
            <SelectTrigger className="h-7 text-xs w-32">
              <SelectValue placeholder="Select..." />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={NONE}>-- none --</SelectItem>
              {columns.map((c) => (
                <SelectItem key={c} value={c}>
                  {c}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex items-center gap-1.5">
          <Label className="text-[10px] text-muted-foreground">Edge Label</Label>
          <Select value={edgeLabelCol} onValueChange={setEdgeLabelCol}>
            <SelectTrigger className="h-7 text-xs w-32">
              <SelectValue placeholder="Select..." />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={NONE}>-- none --</SelectItem>
              {columns.map((c) => (
                <SelectItem key={c} value={c}>
                  {c}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <span className="text-[10px] text-muted-foreground ml-auto">
          {graphData.nodes.length} nodes &middot; {graphData.links.length} edges
        </span>
      </div>

      <div ref={containerRef} className="flex-1 min-h-0 bg-[#0f172a]">
        {!hasMapping ? (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            Select source and target columns to visualize the graph.
          </div>
        ) : graphData.nodes.length === 0 ? (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            No graph data found with current column mapping.
          </div>
        ) : (
          <ForceGraph2D
            graphData={graphData}
            width={dimensions.width}
            height={dimensions.height}
            nodeCanvasObject={nodeCanvasObject}
            nodePointerAreaPaint={(node: GraphNode & { x?: number; y?: number }, color, ctx) => {
              const r = 8;
              ctx.fillStyle = color;
              ctx.beginPath();
              ctx.arc(node.x ?? 0, node.y ?? 0, r, 0, 2 * Math.PI, false);
              ctx.fill();
            }}
            linkDirectionalArrowLength={4}
            linkDirectionalArrowRelPos={1}
            linkColor={() => "rgba(148, 163, 184, 0.4)"}
            linkWidth={1}
            backgroundColor="#0f172a"
            cooldownTicks={100}
          />
        )}
      </div>
    </div>
  );
}
