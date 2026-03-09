export type ErrorCategory =
  | "syntax_error"
  | "table_not_found"
  | "column_not_found"
  | "type_error"
  | "file_error"
  | "auth_error"
  | "export_error"
  | "unknown";

export interface ParsedError {
  message: string;
  code: string | null;
  category: ErrorCategory;
  title: string;
  suggestions: string[];
  raw: unknown;
}

/**
 * Extracts a structured error from any thrown value.
 * Handles Tauri IPC errors ({error, message, code}), standard Error objects,
 * plain strings, and arbitrary objects.
 */
export function extractError(e: unknown): ParsedError {
  let message: string;
  let code: string | null = null;

  if (typeof e === "string") {
    message = e;
  } else if (e instanceof Error) {
    message = e.message;
  } else if (e && typeof e === "object") {
    const obj = e as Record<string, unknown>;
    if (typeof obj.message === "string") {
      message = obj.message;
      if (typeof obj.code === "string") code = obj.code;
    } else {
      try {
        message = JSON.stringify(e, null, 2);
      } catch {
        message = String(e);
      }
    }
  } else {
    message = String(e ?? "Unknown error");
  }

  const classified = classifyError(message, code);

  return {
    message,
    code,
    category: classified.category,
    title: classified.title,
    suggestions: classified.suggestions,
    raw: e,
  };
}

function classifyError(
  message: string,
  code: string | null
): { category: ErrorCategory; title: string; suggestions: string[] } {
  const lower = message.toLowerCase();

  if (lower.includes("parser error") || lower.includes("syntax error")) {
    const position = extractPosition(message);
    return {
      category: "syntax_error",
      title: "SQL Syntax Error",
      suggestions: [
        ...(position
          ? [`Error near position ${position} in your query`]
          : []),
        "Check for missing or extra commas, parentheses, or quotes",
        "String literals use single quotes: 'value' (not double quotes)",
        "Table names with special characters need double quotes: \"my-table\"",
      ],
    };
  }

  if (
    (lower.includes("does not exist") || lower.includes("not found")) &&
    (lower.includes("table") ||
      lower.includes("view") ||
      lower.includes("catalog") ||
      lower.includes("relation"))
  ) {
    const tableName = extractTableName(message);
    return {
      category: "table_not_found",
      title: "Table Not Found",
      suggestions: [
        ...(tableName
          ? [`No table or view named "${tableName}" exists in the database`]
          : []),
        "Use the view name shown under each data source in the sidebar",
        "Query syntax: SELECT * FROM your_view_name",
        "Create a data source first via 'Add Source' if you haven't already",
      ],
    };
  }

  if (
    lower.includes("binder error") &&
    (lower.includes("column") || lower.includes("referenced"))
  ) {
    const colName = extractColumnName(message);
    return {
      category: "column_not_found",
      title: "Column Not Found",
      suggestions: [
        ...(colName
          ? [`Column "${colName}" does not exist in the referenced table`]
          : []),
        "Expand the schema panel to see available column names",
        "Column names are case-insensitive in DuckDB, but check spelling",
        "Use double quotes for column names with spaces: \"column name\"",
      ],
    };
  }

  if (
    lower.includes("conversion error") ||
    lower.includes("type mismatch") ||
    (lower.includes("binder error") && lower.includes("type"))
  ) {
    return {
      category: "type_error",
      title: "Type Error",
      suggestions: [
        "Values don't match the expected column type",
        "Use CAST(column AS type) to convert: CAST(price AS DOUBLE)",
        "Common types: INTEGER, DOUBLE, VARCHAR, BOOLEAN, DATE, TIMESTAMP",
      ],
    };
  }

  if (
    code === "FILE_ERROR" ||
    lower.includes("io error") ||
    lower.includes("no such file") ||
    lower.includes("permission denied") ||
    lower.includes("could not open") ||
    lower.includes("unable to open")
  ) {
    return {
      category: "file_error",
      title: "File Error",
      suggestions: [
        "Verify the file path exists and is accessible",
        "Check that the file isn't open in another application",
        "Ensure the file format matches the selected type",
      ],
    };
  }

  if (
    code === "AUTH_ERROR" ||
    lower.includes("authentication") ||
    lower.includes("access denied") ||
    lower.includes("forbidden") ||
    lower.includes("credential") ||
    lower.includes("httpfs")
  ) {
    return {
      category: "auth_error",
      title: "Authentication Error",
      suggestions: [
        "Verify your access key and secret key are correct",
        "Check that the bucket name, region, and endpoint are correct",
        "Ensure your credentials have read access to the target bucket",
      ],
    };
  }

  if (code === "EXPORT_ERROR") {
    return {
      category: "export_error",
      title: "Export Error",
      suggestions: [
        "Check that the output directory exists and is writable",
        "Verify the query returns data before exporting",
        "Try a different export format (CSV, Parquet, JSON)",
      ],
    };
  }

  return {
    category: "unknown",
    title: "Error",
    suggestions: [],
  };
}

function extractPosition(message: string): string | null {
  const match = message.match(
    /(?:at or near|position)\s*[":]*\s*(\d+|"[^"]*")/i
  );
  return match?.[1] ?? null;
}

function extractTableName(message: string): string | null {
  const patterns = [
    /Table with name "?([^"]+)"? does not exist/i,
    /(?:Table|View|Relation) "([^"]+)" does not exist/i,
    /Catalog Error:.*"([^"]+)"/i,
    /not found:.*"([^"]+)"/i,
  ];
  for (const p of patterns) {
    const m = message.match(p);
    if (m?.[1]) return m[1];
  }
  return null;
}

function extractColumnName(message: string): string | null {
  const patterns = [
    /column "([^"]+)"/i,
    /Referenced column "([^"]+)"/i,
    /Could not find column.*"([^"]+)"/i,
  ];
  for (const p of patterns) {
    const m = message.match(p);
    if (m?.[1]) return m[1];
  }
  return null;
}
