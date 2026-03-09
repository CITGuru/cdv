import { useMemo, useRef, useState } from "react";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ArrowUp, ArrowDown } from "lucide-react";
import { cn } from "@/lib/utils";

interface DataTableProps {
  columns: string[];
  rows: Record<string, unknown>[];
}

export function DataTable({ columns, rows }: DataTableProps) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const parentRef = useRef<HTMLDivElement>(null);

  const columnDefs = useMemo<ColumnDef<Record<string, unknown>>[]>(
    () =>
      columns.map((col) => ({
        accessorKey: col,
        header: col,
        cell: (info) => {
          const val = info.getValue();
          if (val === null || val === undefined) {
            return <span className="text-muted-foreground/50 italic">NULL</span>;
          }
          if (typeof val === "boolean") {
            return (
              <span className={val ? "text-green-500" : "text-destructive"}>
                {String(val)}
              </span>
            );
          }
          if (typeof val === "number") {
            return <span className="tabular-nums">{val.toLocaleString()}</span>;
          }
          return String(val);
        },
        size: Math.max(100, Math.min(col.length * 9 + 60, 280)),
      })),
    [columns]
  );

  const table = useReactTable({
    data: rows,
    columns: columnDefs,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    columnResizeMode: "onChange",
  });

  const { rows: tableRows } = table.getRowModel();

  const virtualizer = useVirtualizer({
    count: tableRows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 28,
    overscan: 50,
  });

  if (columns.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
        No data to display
      </div>
    );
  }

  return (
    <div ref={parentRef} className="overflow-auto h-full">
      <table className="w-max min-w-full border-collapse text-xs">
        <thead className="sticky top-0 z-10 bg-card">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              <th className="text-left px-2 py-1.5 text-[10px] font-medium text-muted-foreground border-b border-border w-12 tabular-nums">
                #
              </th>
              {headerGroup.headers.map((header) => (
                <th
                  key={header.id}
                  className="text-left px-2.5 py-1.5 text-[11px] font-semibold text-muted-foreground border-b border-border select-none relative group"
                  style={{ width: header.getSize() }}
                >
                  <div
                    className="flex items-center gap-1 cursor-pointer hover:text-foreground transition-colors"
                    onClick={header.column.getToggleSortingHandler()}
                  >
                    <span className="font-mono">
                      {flexRender(
                        header.column.columnDef.header,
                        header.getContext()
                      )}
                    </span>
                    {header.column.getIsSorted() === "asc" && (
                      <ArrowUp className="size-3" />
                    )}
                    {header.column.getIsSorted() === "desc" && (
                      <ArrowDown className="size-3" />
                    )}
                  </div>
                  <div
                    onMouseDown={header.getResizeHandler()}
                    onTouchStart={header.getResizeHandler()}
                    className={cn(
                      "absolute right-0 top-0 h-full w-1 cursor-col-resize opacity-0 group-hover:opacity-100 transition-opacity",
                      "hover:bg-primary bg-border"
                    )}
                  />
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {virtualizer.getVirtualItems().length > 0 && (
            <tr style={{ height: virtualizer.getVirtualItems()[0]?.start ?? 0 }}>
              <td />
            </tr>
          )}
          {virtualizer.getVirtualItems().map((virtualRow) => {
            const row = tableRows[virtualRow.index];
            return (
              <tr
                key={row.id}
                className="border-b border-border/50 hover:bg-muted/30 transition-colors"
              >
                <td className="px-2 py-1 text-[10px] text-muted-foreground/60 font-mono tabular-nums">
                  {virtualRow.index + 1}
                </td>
                {row.getVisibleCells().map((cell) => (
                  <td
                    key={cell.id}
                    className="px-2.5 py-1 text-xs font-mono truncate max-w-xs"
                    style={{ width: cell.column.getSize() }}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            );
          })}
          {virtualizer.getVirtualItems().length > 0 && (
            <tr
              style={{
                height:
                  virtualizer.getTotalSize() -
                  (virtualizer.getVirtualItems().at(-1)?.end ?? 0),
              }}
            >
              <td />
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
