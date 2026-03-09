import { tableFromIPC, type Vector } from "apache-arrow";
import type { QueryResult } from "./types";

export function decodeArrowIPC(buffer: number[]): QueryResult {
  if (!buffer || buffer.length === 0) {
    return { columns: [], rows: [] };
  }

  const bytes = new Uint8Array(buffer);
  const table = tableFromIPC(bytes);

  const columns = table.schema.fields.map((f) => f.name);
  const numRows = table.numRows;
  const numCols = columns.length;

  const vectors: (Vector | null)[] = new Array(numCols);
  for (let c = 0; c < numCols; c++) {
    vectors[c] = table.getChild(columns[c]);
  }

  const rows: Record<string, unknown>[] = new Array(numRows);
  for (let i = 0; i < numRows; i++) {
    const row: Record<string, unknown> = {};
    for (let c = 0; c < numCols; c++) {
      const val = vectors[c]?.get(i);
      row[columns[c]] = val !== undefined && val !== null && typeof val === "bigint"
        ? Number(val)
        : val;
    }
    rows[i] = row;
  }

  return { columns, rows };
}
