import { useState } from "react";
import { ChevronRight, ChevronDown, Columns3, Rows3 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { DataSource } from "@/lib/types";

interface SchemaPanelProps {
  dataset: DataSource;
}

export function SchemaPanel({ dataset }: SchemaPanelProps) {
  const [collapsed, setCollapsed] = useState(true);

  return (
    <div className="border-b border-border bg-card shrink-0">
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="w-full flex items-center justify-between px-4 py-2 text-sm hover:bg-muted/50 transition-colors"
      >
        <div className="flex items-center gap-2">
          {collapsed ? (
            <ChevronRight className="size-3.5 text-muted-foreground" />
          ) : (
            <ChevronDown className="size-3.5 text-muted-foreground" />
          )}
          <span className="font-medium">{dataset.name}</span>
          <Badge variant="secondary" className="font-mono text-[10px]">
            {dataset.view_name}
          </Badge>
          <Badge variant="outline" className="font-mono text-[10px]">
            {dataset.format.toUpperCase()}
          </Badge>
        </div>
        <div className="flex items-center gap-3 text-xs text-muted-foreground">
          <span className="flex items-center gap-1">
            <Columns3 className="size-3" />
            {dataset.schema.length}
          </span>
          <span className="flex items-center gap-1">
            <Rows3 className="size-3" />
            {dataset.row_count?.toLocaleString() ?? "?"}
          </span>
        </div>
      </button>

      {!collapsed && (
        <div className="px-4 pb-3 max-h-48 overflow-auto">
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="h-7 text-xs">#</TableHead>
                <TableHead className="h-7 text-xs">Column</TableHead>
                <TableHead className="h-7 text-xs">Type</TableHead>
                <TableHead className="h-7 text-xs">Nullable</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {dataset.schema.map((col, idx) => (
                <TableRow key={col.name} className="hover:bg-muted/30">
                  <TableCell className="py-1 text-xs text-muted-foreground font-mono tabular-nums">
                    {idx + 1}
                  </TableCell>
                  <TableCell className="py-1 text-xs font-mono font-medium">
                    {col.name}
                  </TableCell>
                  <TableCell className="py-1">
                    <Badge variant="outline" className="font-mono text-[10px] px-1.5 py-0 h-5">
                      {col.data_type}
                    </Badge>
                  </TableCell>
                  <TableCell className="py-1 text-xs">
                    {col.nullable ? (
                      <span className="text-yellow-500">YES</span>
                    ) : (
                      <span className="text-muted-foreground">NO</span>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
