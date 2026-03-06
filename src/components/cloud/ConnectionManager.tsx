import { useState } from "react";
import type { DatasetInfo, S3Config } from "../../lib/types";
import { connectS3, listBucketFiles, openRemoteDataset } from "../../lib/ipc";

interface ConnectionManagerProps {
  onClose: () => void;
  onDatasetOpen: (dataset: DatasetInfo) => void;
}

export function ConnectionManager({ onClose, onDatasetOpen }: ConnectionManagerProps) {
  const [config, setConfig] = useState<S3Config>({
    endpoint: "",
    bucket: "",
    region: "us-east-1",
    access_key: "",
    secret_key: "",
    prefix: "",
  });
  const [files, setFiles] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [connected, setConnected] = useState(false);

  const handleConnect = async () => {
    setLoading(true);
    setError(null);
    try {
      await connectS3({
        ...config,
        endpoint: config.endpoint || undefined,
        prefix: config.prefix || undefined,
      });
      const fileList = await listBucketFiles(config.bucket, config.prefix || undefined);
      setFiles(fileList);
      setConnected(true);
    } catch (e) {
      setError(typeof e === "string" ? e : String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleOpenFile = async (path: string) => {
    setLoading(true);
    setError(null);
    try {
      const dataset = await openRemoteDataset(path);
      onDatasetOpen(dataset);
    } catch (e) {
      setError(typeof e === "string" ? e : String(e));
    } finally {
      setLoading(false);
    }
  };

  const updateField = (field: keyof S3Config, value: string) => {
    setConfig((prev) => ({ ...prev, [field]: value }));
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div className="bg-zinc-900 border border-zinc-700 rounded-lg shadow-2xl w-full max-w-lg max-h-[80vh] flex flex-col">
        <div className="flex items-center justify-between px-5 py-4 border-b border-zinc-700">
          <h2 className="text-sm font-semibold text-zinc-200">S3 Connection</h2>
          <button
            onClick={onClose}
            className="text-zinc-500 hover:text-zinc-300 text-lg"
          >
            ✕
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-5">
          {error && (
            <div className="mb-4 px-3 py-2 bg-red-900/30 border border-red-800 rounded text-sm text-red-300">
              {error}
            </div>
          )}

          {!connected ? (
            <div className="space-y-3">
              <Field label="Endpoint (optional)" value={config.endpoint ?? ""} onChange={(v) => updateField("endpoint", v)} placeholder="s3.amazonaws.com" />
              <Field label="Bucket" value={config.bucket} onChange={(v) => updateField("bucket", v)} placeholder="my-bucket" />
              <Field label="Region" value={config.region} onChange={(v) => updateField("region", v)} placeholder="us-east-1" />
              <Field label="Access Key" value={config.access_key} onChange={(v) => updateField("access_key", v)} type="password" />
              <Field label="Secret Key" value={config.secret_key} onChange={(v) => updateField("secret_key", v)} type="password" />
              <Field label="Path Prefix (optional)" value={config.prefix ?? ""} onChange={(v) => updateField("prefix", v)} placeholder="data/" />

              <button
                onClick={handleConnect}
                disabled={loading || !config.bucket || !config.access_key || !config.secret_key}
                className="w-full mt-2 px-4 py-2.5 bg-blue-600 hover:bg-blue-500 disabled:bg-zinc-700 disabled:text-zinc-500 text-white text-sm font-medium rounded transition-colors"
              >
                {loading ? "Connecting..." : "Test & Connect"}
              </button>
            </div>
          ) : (
            <div>
              <h3 className="text-xs font-semibold text-zinc-400 uppercase mb-2">
                Files in s3://{config.bucket}
              </h3>
              {files.length === 0 && (
                <p className="text-sm text-zinc-500">No files found</p>
              )}
              <ul className="space-y-1">
                {files.map((f) => (
                  <li key={f}>
                    <button
                      onClick={() => handleOpenFile(f)}
                      disabled={loading}
                      className="w-full text-left px-3 py-2 rounded text-sm text-zinc-300 hover:bg-zinc-800 transition-colors truncate"
                    >
                      {f}
                    </button>
                  </li>
                ))}
              </ul>
              <button
                onClick={() => { setConnected(false); setFiles([]); }}
                className="mt-4 text-xs text-zinc-500 hover:text-zinc-300"
              >
                ← Back to config
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function Field({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: string;
}) {
  return (
    <div>
      <label className="block text-xs text-zinc-400 mb-1">{label}</label>
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded text-sm text-zinc-200 placeholder-zinc-600 focus:outline-none focus:border-blue-500"
      />
    </div>
  );
}
