import { Settings } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import type { Settings as SettingsType } from "@/lib/types";

interface SettingsModalProps {
  open: boolean;
  onClose: () => void;
  settings: SettingsType;
  onUpdate: (next: Partial<SettingsType>) => void;
}

const SIDEBAR_MIN = 180;
const SIDEBAR_MAX = 480;

export function SettingsModal({
  open,
  onClose,
  settings,
  onUpdate,
}: SettingsModalProps) {
  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-md max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Settings className="size-4" />
            Settings
          </DialogTitle>
          <DialogDescription>
            App settings are saved automatically. Changes apply immediately.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 py-2">
          <section className="space-y-3">
            <h3 className="text-sm font-medium text-foreground">Layout</h3>
            <div className="grid gap-2">
              <Label htmlFor="sidebar_width">Sidebar width (px)</Label>
              <Input
                id="sidebar_width"
                type="number"
                min={SIDEBAR_MIN}
                max={SIDEBAR_MAX}
                value={settings.sidebar_width}
                onChange={(e) => {
                  const n = parseInt(e.target.value, 10);
                  if (!Number.isNaN(n))
                    onUpdate({
                      sidebar_width: Math.max(
                        SIDEBAR_MIN,
                        Math.min(SIDEBAR_MAX, n)
                      ),
                    });
                }}
              />
            </div>
          </section>

          <section className="space-y-3">
            <h3 className="text-sm font-medium text-foreground">Data / Query</h3>
            <div className="grid gap-2">
              <Label htmlFor="default_page_size">Default page size</Label>
              <Input
                id="default_page_size"
                type="number"
                min={1}
                max={100_000}
                value={settings.default_page_size}
                onChange={(e) => {
                  const n = parseInt(e.target.value, 10);
                  if (!Number.isNaN(n) && n > 0)
                    onUpdate({ default_page_size: n });
                }}
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="max_rows_per_query">Max rows per query</Label>
              <Input
                id="max_rows_per_query"
                type="number"
                min={100}
                value={settings.max_rows_per_query}
                onChange={(e) => {
                  const n = parseInt(e.target.value, 10);
                  if (!Number.isNaN(n) && n > 0)
                    onUpdate({ max_rows_per_query: n });
                }}
              />
            </div>
            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="streaming_enabled"
                checked={settings.streaming_enabled}
                onChange={(e) =>
                  onUpdate({ streaming_enabled: e.target.checked })
                }
                className="h-4 w-4 rounded border-input"
              />
              <Label htmlFor="streaming_enabled" className="font-normal">
                Enable streaming for large results
              </Label>
            </div>
            {settings.streaming_enabled && (
              <div className="grid gap-2 pl-6">
                <Label htmlFor="streaming_threshold">
                  Streaming threshold (rows)
                </Label>
                <Input
                  id="streaming_threshold"
                  type="number"
                  min={1000}
                  value={settings.streaming_threshold ?? 10_000}
                  onChange={(e) => {
                    const n = parseInt(e.target.value, 10);
                    if (!Number.isNaN(n) && n > 0)
                      onUpdate({ streaming_threshold: n });
                  }}
                />
              </div>
            )}
          </section>

          <section className="space-y-3">
            <h3 className="text-sm font-medium text-foreground">Export</h3>
            <div className="grid gap-2">
              <Label htmlFor="default_export_format">
                Default export format
              </Label>
              <select
                id="default_export_format"
                value={settings.default_export_format}
                onChange={(e) =>
                  onUpdate({
                    default_export_format: e.target.value as
                      | "csv"
                      | "parquet"
                      | "json",
                  })
                }
                className="h-8 w-full rounded-lg border border-input bg-transparent px-2.5 text-sm"
              >
                <option value="csv">CSV</option>
                <option value="parquet">Parquet</option>
                <option value="json">JSON</option>
              </select>
            </div>
          </section>
        </div>

        <div className="flex justify-end pt-2">
          <Button variant="outline" onClick={onClose}>
            Done
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
