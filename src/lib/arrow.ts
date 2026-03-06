import { tableFromIPC } from "apache-arrow";
import type { QueryResult } from "./types";

export function decodeArrowIPC(buffer: number[]): QueryResult {
  if (!buffer || buffer.length === 0) {
    return { columns: [], rows: [] };
  }

  const bytes = new Uint8Array(buffer);
  const table = tableFromIPC(bytes);

  const columns = table.schema.fields.map((f) => f.name);
  const rows: Record<string, unknown>[] = [];

  for (let i = 0; i < table.numRows; i++) {
    const row: Record<string, unknown> = {};
    for (const col of columns) {
      const val = table.getChild(col)?.get(i);
      row[col] = val !== undefined && val !== null && typeof val === "bigint"
        ? Number(val)
        : val;
    }
    rows.push(row);
  }

  return { columns, rows };
}
