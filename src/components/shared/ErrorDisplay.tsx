import { useState } from "react";
import {
  AlertCircle,
  FileWarning,
  SearchX,
  Columns3,
  ShieldAlert,
  Code2,
  ChevronDown,
  ChevronRight,
  Lightbulb,
  Copy,
  Check,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import type { ParsedError, ErrorCategory } from "@/lib/errors";

interface ErrorDisplayProps {
  error: ParsedError;
  compact?: boolean;
}

const CATEGORY_CONFIG: Record<
  ErrorCategory,
  { icon: typeof AlertCircle; color: string; bg: string; border: string }
> = {
  syntax_error: {
    icon: Code2,
    color: "text-orange-400",
    bg: "bg-orange-500/10",
    border: "border-orange-500/30",
  },
  table_not_found: {
    icon: SearchX,
    color: "text-yellow-400",
    bg: "bg-yellow-500/10",
    border: "border-yellow-500/30",
  },
  column_not_found: {
    icon: Columns3,
    color: "text-yellow-400",
    bg: "bg-yellow-500/10",
    border: "border-yellow-500/30",
  },
  type_error: {
    icon: AlertCircle,
    color: "text-purple-400",
    bg: "bg-purple-500/10",
    border: "border-purple-500/30",
  },
  file_error: {
    icon: FileWarning,
    color: "text-red-400",
    bg: "bg-red-500/10",
    border: "border-red-500/30",
  },
  auth_error: {
    icon: ShieldAlert,
    color: "text-red-400",
    bg: "bg-red-500/10",
    border: "border-red-500/30",
  },
  export_error: {
    icon: FileWarning,
    color: "text-red-400",
    bg: "bg-red-500/10",
    border: "border-red-500/30",
  },
  connector_error: {
    icon: AlertCircle,
    color: "text-orange-400",
    bg: "bg-orange-500/10",
    border: "border-orange-500/30",
  },
  unknown: {
    icon: AlertCircle,
    color: "text-destructive",
    bg: "bg-destructive/10",
    border: "border-destructive/30",
  },
};

export function ErrorDisplay({ error, compact = false }: ErrorDisplayProps) {
  const config = CATEGORY_CONFIG[error.category];
  const Icon = config.icon;

  if (compact) {
    return <CompactError error={error} config={config} Icon={Icon} />;
  }

  return <FullError error={error} config={config} Icon={Icon} />;
}

function CompactError({
  error,
  config,
  Icon,
}: {
  error: ParsedError;
  config: (typeof CATEGORY_CONFIG)[ErrorCategory];
  Icon: typeof AlertCircle;
}) {
  return (
    <div
      className={`flex items-start gap-2 px-3 py-2 ${config.bg} border ${config.border} rounded-md text-sm`}
    >
      <Icon className={`size-4 shrink-0 mt-0.5 ${config.color}`} />
      <div className="min-w-0 flex-1">
        <span className={`font-medium ${config.color}`}>{error.title}: </span>
        <span className="text-foreground/80">{error.message}</span>
        {error.suggestions.length > 0 && (
          <p className="text-xs text-muted-foreground mt-1">
            {error.suggestions[0]}
          </p>
        )}
      </div>
    </div>
  );
}

function FullError({
  error,
  config,
  Icon,
}: {
  error: ParsedError;
  config: (typeof CATEGORY_CONFIG)[ErrorCategory];
  Icon: typeof AlertCircle;
}) {
  const [showDetails, setShowDetails] = useState(false);
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    const text = [
      `${error.title} (${error.code ?? error.category})`,
      error.message,
    ].join("\n");
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  return (
    <div className={`${config.bg} border ${config.border} rounded-lg`}>
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2.5">
        <div className="flex items-center gap-2">
          <Icon className={`size-4 ${config.color}`} />
          <span className={`text-sm font-semibold ${config.color}`}>
            {error.title}
          </span>
          {error.code && (
            <span className="text-[10px] font-mono text-muted-foreground px-1.5 py-0.5 bg-muted rounded">
              {error.code}
            </span>
          )}
        </div>
        <Button
          variant="ghost"
          size="icon-xs"
          onClick={handleCopy}
          className="shrink-0"
        >
          {copied ? (
            <Check className="size-3 text-green-500" />
          ) : (
            <Copy className="size-3 text-muted-foreground" />
          )}
        </Button>
      </div>

      {/* Message */}
      <div className="px-4 pb-3">
        <pre className="text-xs font-mono text-foreground/90 whitespace-pre-wrap break-words leading-relaxed">
          {error.message}
        </pre>
      </div>

      {/* Suggestions */}
      {error.suggestions.length > 0 && (
        <div className="border-t border-border/40 px-4 py-2.5">
          <div className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground mb-2">
            <Lightbulb className="size-3" />
            Suggestions
          </div>
          <ul className="space-y-1">
            {error.suggestions.map((s, i) => (
              <li
                key={i}
                className="text-xs text-foreground/70 flex items-start gap-2"
              >
                <span className="text-muted-foreground mt-0.5 shrink-0">
                  &bull;
                </span>
                {s}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Collapsible raw details */}
      {error.raw != null && typeof error.raw === "object" ? (
        <div className="border-t border-border/40">
          <button
            onClick={() => setShowDetails(!showDetails)}
            className="flex items-center gap-1.5 px-4 py-2 text-[10px] text-muted-foreground hover:text-foreground transition-colors w-full"
          >
            {showDetails ? (
              <ChevronDown className="size-3" />
            ) : (
              <ChevronRight className="size-3" />
            )}
            Raw details
          </button>
          {showDetails && (
            <pre className="px-4 pb-3 text-[10px] font-mono text-muted-foreground whitespace-pre-wrap break-all">
              {JSON.stringify(error.raw, null, 2)}
            </pre>
          )}
        </div>
      ) : null}
    </div>
  );
}
