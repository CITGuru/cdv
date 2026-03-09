import { useState, useCallback, useEffect } from "react";
import type { Settings } from "@/lib/types";
import { getSettings, setSettings } from "@/lib/ipc";

const DEFAULT_SETTINGS: Settings = {
  sidebar_width: 256,
  default_page_size: 1000,
  max_rows_per_query: 10_000,
  default_export_format: "csv",
  streaming_enabled: false,
  streaming_threshold: 10_000,
};

export function useSettings() {
  const [settings, setSettingsState] = useState<Settings | null>(null);

  useEffect(() => {
    getSettings()
      .then((s) => setSettingsState(s))
      .catch(() => setSettingsState(DEFAULT_SETTINGS));
  }, []);

  const updateSettings = useCallback((next: Partial<Settings> | ((prev: Settings) => Settings)) => {
    setSettingsState((prev) => {
      const nextSettings =
        typeof next === "function"
          ? next(prev ?? DEFAULT_SETTINGS)
          : { ...(prev ?? DEFAULT_SETTINGS), ...next };
      setSettings(nextSettings).catch(() => {});
      return nextSettings;
    });
  }, []);

  return {
    settings: settings ?? DEFAULT_SETTINGS,
    isLoaded: settings !== null,
    updateSettings,
  };
}
