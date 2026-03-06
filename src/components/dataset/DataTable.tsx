import { useMemo, useRef } from "react";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useState } from "react";

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
            return <span className="text-zinc-600 italic">null</span>;
          }
          return String(val);
        },
        size: Math.max(120, Math.min(col.length * 10 + 40, 300)),
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
    estimateSize: () => 32,
    overscan: 50,
  });

  if (columns.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-zinc-500 text-sm">
        No data to display
      </div>
    );
  }

  return (
    <div ref={parentRef} className="overflow-auto h-full">
      <table className="w-max min-w-full text-sm border-collapse">
        <thead className="sticky top-0 z-10 bg-zinc-900">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <th
                  key={header.id}
                  className="text-left px-3 py-2 text-xs font-semibold text-zinc-400 border-b border-zinc-700 select-none relative"
                  style={{ width: header.getSize() }}
                >
                  <div
                    className="flex items-center gap-1 cursor-pointer hover:text-zinc-200"
                    onClick={header.column.getToggleSortingHandler()}
                  >
                    {flexRender(
                      header.column.columnDef.header,
                      header.getContext()
                    )}
                    {{
                      asc: " ↑",
                      desc: " ↓",
                    }[header.column.getIsSorted() as string] ?? ""}
                  </div>
                  <div
                    onMouseDown={header.getResizeHandler()}
                    onTouchStart={header.getResizeHandler()}
                    className="absolute right-0 top-0 h-full w-1 cursor-col-resize hover:bg-blue-500"
                  />
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          <tr style={{ height: virtualizer.getVirtualItems()[0]?.start ?? 0 }}>
            <td />
          </tr>
          {virtualizer.getVirtualItems().map((virtualRow) => {
            const row = tableRows[virtualRow.index];
            return (
              <tr
                key={row.id}
                className="border-b border-zinc-800/50 hover:bg-zinc-800/30"
              >
                {row.getVisibleCells().map((cell) => (
                  <td
                    key={cell.id}
                    className="px-3 py-1.5 text-xs text-zinc-300 font-mono truncate max-w-xs"
                    style={{ width: cell.column.getSize() }}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            );
          })}
          <tr
            style={{
              height:
                virtualizer.getTotalSize() -
                (virtualizer.getVirtualItems().at(-1)?.end ?? 0),
            }}
          >
            <td />
          </tr>
        </tbody>
      </table>
    </div>
  );
}
