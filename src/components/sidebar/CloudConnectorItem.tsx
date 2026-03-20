import { Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { Connector } from "@/lib/types";
import { getConnectorIcon, getConnectorIconColor } from "./sidebar-icons";

export function CloudConnectorItem({
  connector,
  onRemove,
}: {
  connector: Connector;
  onRemove: (id: string) => void;
}) {
  const Icon = getConnectorIcon(connector);
  const scheme = connector.connector_type === "gcs" ? "gcs" : connector.connector_type === "r2" ? "r2" : "s3";
  const bucket = connector.config.bucket ?? "";
  const prefix = connector.config.prefix ?? "";
  const iconColor = getConnectorIconColor(connector);
  return (
    <div className="group flex items-center gap-0.5 rounded-md text-sidebar-foreground hover:bg-sidebar-accent/50 transition-colors min-h-7">
      <span className="w-5 shrink-0" aria-hidden />
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="flex-1 flex items-center gap-2 px-1.5 py-1 min-w-0">
            <Icon className={`size-4 shrink-0 ${iconColor}`} />
            <span className="truncate text-xs font-medium flex-1">{connector.name}</span>
            <span className={`shrink-0 text-[9px] px-1.5 py-0.5 rounded-sm font-medium bg-muted ${iconColor}`}>
              {connector.connector_type.toUpperCase()}
            </span>
          </div>
        </TooltipTrigger>
        <TooltipContent side="right">
          <p className="text-xs font-mono">
            {scheme}://{bucket}
            {prefix ? `/${prefix}` : ""}
          </p>
          <p className="text-xs text-muted-foreground mt-0.5">
            {connector.connector_type.toUpperCase()} · {connector.config.region ?? ""}
          </p>
        </TooltipContent>
      </Tooltip>
      <Button
        variant="ghost"
        size="icon-xs"
        className="opacity-0 group-hover:opacity-100 shrink-0 h-6 w-6"
        onClick={() => onRemove(connector.id)}
      >
        <Trash2 className="size-3 text-muted-foreground hover:text-destructive" />
      </Button>
    </div>
  );
}
