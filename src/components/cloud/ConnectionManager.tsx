import { useState } from "react";
import { Loader2, Cloud, CheckCircle2 } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import type { Connector } from "@/lib/types";
import type { ParsedError } from "@/lib/errors";
import { extractError } from "@/lib/errors";
import { ErrorDisplay } from "@/components/shared/ErrorDisplay";
import type { ConnectionProvider } from "@/lib/ipc";
import { createConnection } from "@/lib/ipc";

const PROVIDERS: { value: ConnectionProvider; label: string }[] = [
  { value: "s3", label: "Amazon S3" },
  { value: "gcp", label: "Google Cloud Storage" },
  { value: "cloudflare", label: "Cloudflare R2" },
];

interface ConnectionManagerProps {
  onClose: () => void;
  onCreated: (conn: Connector) => void;
}

export function ConnectionManager({ onClose, onCreated }: ConnectionManagerProps) {
  const [provider, setProvider] = useState<ConnectionProvider>("s3");
  const [name, setName] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [bucket, setBucket] = useState("");
  const [region, setRegion] = useState(provider === "cloudflare" ? "auto" : "us-east-1");
  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [prefix, setPrefix] = useState("");
  const [accountId, setAccountId] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ParsedError | null>(null);
  const [success, setSuccess] = useState(false);

  const handleProviderChange = (p: ConnectionProvider) => {
    setProvider(p);
    if (p === "cloudflare") setRegion("auto");
    else if (region === "auto") setRegion("us-east-1");
  };

  const handleCreate = async () => {
    setLoading(true);
    setError(null);
    try {
      const conn = await createConnection({
        name,
        provider,
        endpoint: provider === "s3" ? endpoint || undefined : undefined,
        bucket,
        region,
        accessKey,
        secretKey,
        prefix: prefix || undefined,
        accountId: provider === "cloudflare" ? accountId || undefined : undefined,
      });
      setSuccess(true);
      onCreated(conn);
      setTimeout(() => onClose(), 800);
    } catch (e) {
      setError(extractError(e));
    } finally {
      setLoading(false);
    }
  };

  const needsEndpoint = provider === "s3";
  const needsRegion = provider !== "gcp";
  const needsAccountId = provider === "cloudflare";

  const canSubmit =
    name.trim() &&
    bucket.trim() &&
    accessKey.trim() &&
    secretKey.trim() &&
    (!needsAccountId || accountId.trim());

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="w-[95vw] max-w-6xl sm:max-w-6xl min-h-[60vh] max-h-[90vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Cloud className="size-4" />
            New Cloud Connection
          </DialogTitle>
          <DialogDescription>
            Create a named connection with scoped credentials via DuckDB&apos;s HTTPFS extension.
          </DialogDescription>
        </DialogHeader>

        {error && <ErrorDisplay error={error} compact />}

        {success && (
          <div className="flex items-center gap-2 px-3 py-2 bg-green-500/10 border border-green-500/30 rounded-md text-sm text-green-500">
            <CheckCircle2 className="size-4 shrink-0" />
            Connection created!
          </div>
        )}

        <div className="space-y-3">
          <div>
            <Label className="text-xs">Provider</Label>
            <div className="flex gap-1 p-0.5 bg-muted rounded-md mt-1">
              {PROVIDERS.map((p) => (
                <button
                  key={p.value}
                  type="button"
                  onClick={() => handleProviderChange(p.value)}
                  className={`flex-1 px-3 py-2 rounded text-sm font-medium transition-colors ${
                    provider === p.value
                      ? "bg-background text-foreground shadow-sm"
                      : "text-muted-foreground hover:text-foreground"
                  }`}
                >
                  {p.label}
                </button>
              ))}
            </div>
          </div>

          <div>
            <Label htmlFor="conn-name" className="text-xs">Connection Name</Label>
            <Input
              id="conn-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder={
                provider === "s3"
                  ? "e.g. Production S3"
                  : provider === "gcp"
                    ? "e.g. Analytics GCS"
                    : "e.g. Assets R2"
              }
              className="mt-1"
            />
          </div>

          {needsAccountId && (
            <div>
              <Label htmlFor="conn-account-id" className="text-xs">
                Account ID <span className="text-muted-foreground">(R2)</span>
              </Label>
              <Input
                id="conn-account-id"
                value={accountId}
                onChange={(e) => setAccountId(e.target.value)}
                placeholder="abc123def456..."
                className="mt-1 font-mono"
              />
            </div>
          )}

          {needsEndpoint && (
            <div>
              <Label htmlFor="conn-endpoint" className="text-xs">
                Endpoint <span className="text-muted-foreground">(optional, for S3-compatible)</span>
              </Label>
              <Input
                id="conn-endpoint"
                value={endpoint}
                onChange={(e) => setEndpoint(e.target.value)}
                placeholder="s3.amazonaws.com"
                className="mt-1"
              />
            </div>
          )}

          <div className="grid grid-cols-2 gap-3">
            <div>
              <Label htmlFor="conn-bucket" className="text-xs">Bucket</Label>
              <Input
                id="conn-bucket"
                value={bucket}
                onChange={(e) => setBucket(e.target.value)}
                placeholder="my-bucket"
                className="mt-1"
              />
            </div>
            {needsRegion && (
              <div>
                <Label htmlFor="conn-region" className="text-xs">
                  Region {provider === "cloudflare" && <span className="text-muted-foreground">(e.g. auto)</span>}
                </Label>
                <Input
                  id="conn-region"
                  value={region}
                  onChange={(e) => setRegion(e.target.value)}
                  placeholder={provider === "cloudflare" ? "auto" : "us-east-1"}
                  className="mt-1"
                />
              </div>
            )}
            <div>
              <Label htmlFor="conn-ak" className="text-xs">Access Key</Label>
              <Input
                id="conn-ak"
                type="password"
                value={accessKey}
                onChange={(e) => setAccessKey(e.target.value)}
                className="mt-1"
              />
            </div>
            <div>
              <Label htmlFor="conn-sk" className="text-xs">Secret Key</Label>
              <Input
                id="conn-sk"
                type="password"
                value={secretKey}
                onChange={(e) => setSecretKey(e.target.value)}
                className="mt-1"
              />
            </div>
            <div className="col-span-2">
              <Label htmlFor="conn-prefix" className="text-xs">
                Path Prefix <span className="text-muted-foreground">(optional)</span>
              </Label>
              <Input
                id="conn-prefix"
                value={prefix}
                onChange={(e) => setPrefix(e.target.value)}
                placeholder="data/"
                className="mt-1"
              />
            </div>
          </div>

          <Button
            onClick={handleCreate}
            disabled={loading || !canSubmit}
            className="w-full gap-2"
          >
            {loading && <Loader2 className="size-4 animate-spin" />}
            {loading ? "Creating..." : "Create Connection"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
