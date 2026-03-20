import { LayoutGrid } from "lucide-react";
import type { ColumnInfo } from "@/lib/types";

export function ColumnTreeRow({ column }: { column: ColumnInfo }) {
  const keyLabel = column.key ? (column.key === "PRI" ? "PK" : column.key) : null;
  return (
    <div
      className="flex items-center gap-2 py-0.5 pl-5 pr-2 rounded text-xs text-sidebar-foreground/90 min-w-0"
      title={`${column.name}: ${column.data_type}${column.nullable ? " (nullable)" : ""}${keyLabel ? ` [${keyLabel}]` : ""}`}
    >
      <LayoutGrid className="size-3 shrink-0 text-muted-foreground/80" />
      <span className="font-mono truncate text-[11px]">{column.name}</span>
      {keyLabel && (
        <span className="shrink-0 text-[9px] px-1 rounded bg-primary/20 text-primary font-medium">
          {keyLabel}
        </span>
      )}
      <span className="text-muted-foreground font-mono shrink-0 text-[10px]">
        {column.data_type}
      </span>
    </div>
  );
}
