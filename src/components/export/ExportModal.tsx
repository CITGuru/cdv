import { useState } from "react";
import { save } from "@tauri-apps/plugin-dialog";
import { Download, Loader2, CheckCircle2 } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import type { ParsedError } from "@/lib/errors";
import { extractError } from "@/lib/errors";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";
import { exportData } from "@/lib/ipc";

interface ExportModalProps {
  defaultQuery: string;
  defaultFormat?: "csv" | "parquet" | "json";
  onClose: () => void;
}

export function ExportModal({ defaultQuery, defaultFormat = "csv", onClose }: ExportModalProps) {
  const [query, setQuery] = useState(defaultQuery);
  const [format, setFormat] = useState<"csv" | "parquet" | "json">(
    ["csv", "parquet", "json"].includes(defaultFormat) ? defaultFormat : "csv"
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ParsedError | null>(null);
  const [success, setSuccess] = useState(false);

  const handleExport = async () => {
    const ext = format === "parquet" ? "parquet" : format === "json" ? "json" : "csv";
    const outputPath = await save({
      defaultPath: `export.${ext}`,
      filters: [{ name: format.toUpperCase(), extensions: [ext] }],
    });

    if (!outputPath) return;

    setLoading(true);
    setError(null);
    setSuccess(false);

    try {
      await exportData(query, format, outputPath);
      setSuccess(true);
      setTimeout(() => onClose(), 1500);
    } catch (e) {
      setError(extractError(e));
    } finally {
      setLoading(false);
    }
  };

  const formats = [
    { value: "csv" as const, label: "CSV" },
    { value: "parquet" as const, label: "Parquet" },
    { value: "json" as const, label: "JSON" },
  ];

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Download className="size-4" />
            Export Data
          </DialogTitle>
          <DialogDescription>
            Export query results to a local file.
          </DialogDescription>
        </DialogHeader>

        {error && <ErrorDisplay error={error} compact />}

        {success && (
          <div className="flex items-center gap-2 px-3 py-2 bg-green-500/10 border border-green-500/30 rounded-md text-sm text-green-500">
            <CheckCircle2 className="size-4 shrink-0" />
            Export complete!
          </div>
        )}

        <div className="space-y-4">
          <div>
            <Label className="text-xs">Format</Label>
            <div className="flex gap-1.5 mt-1.5">
              {formats.map((f) => (
                <Button
                  key={f.value}
                  variant={format === f.value ? "default" : "outline"}
                  size="sm"
                  onClick={() => setFormat(f.value)}
                  className="flex-1 text-xs"
                >
                  {f.label}
                </Button>
              ))}
            </div>
          </div>

          <div>
            <Label htmlFor="export-query" className="text-xs">Query</Label>
            <Textarea
              id="export-query"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              rows={4}
              className="mt-1.5 font-mono text-xs resize-none"
            />
          </div>

          <Button
            onClick={handleExport}
            disabled={loading || !query.trim()}
            className="w-full gap-2"
          >
            {loading ? (
              <Loader2 className="size-4 animate-spin" />
            ) : (
              <Download className="size-4" />
            )}
            {loading ? "Exporting..." : "Export"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
