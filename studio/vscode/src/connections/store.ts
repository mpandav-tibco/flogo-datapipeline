// Connection management for Data Pipeline Studio.
//
// Connections let users define a data endpoint (Postgres, Kafka, S3, …) ONCE and
// bind it to any source/sink by name — so DSNs and secrets are never typed into
// (or written to) a .datapipeline file.
//
// Persistence is split for safety:
//   • Non-secret fields + metadata → <workspace>/.flogo/connections.json
//   • Secret fields (passwords, keys, tokens) → VS Code Secret Storage
//
// At run/preview time the host resolves a bound connection into concrete engine
// config (dsn / bucket / brokers …) in an in-memory copy of the pipeline, which
// is written to a sibling temp file the engine actually reads. The on-disk
// .datapipeline keeps only `connection: "<name>"`.

import * as vscode from 'vscode';
import * as path from 'path';

export type ConnFamily = 'db' | 'stream' | 'obj' | 'http';

export interface ConnField {
  key: string;
  label: string;
  secret?: boolean;
  optional?: boolean;
  placeholder?: string;
}

export interface ConnType {
  type: string; // postgres, mysql, sqlite, kafka, s3, gcs, adls, http
  label: string;
  family: ConnFamily;
  desc: string;
  fields: ConnField[];
}

// The connection catalogue — mirrors the wireframe's CONN_TYPES.
export const CONN_TYPES: ConnType[] = [
  {
    type: 'postgres', label: 'PostgreSQL — JDBC', family: 'db', desc: 'jdbc · postgres',
    fields: [
      { key: 'host', label: 'Host', placeholder: 'prod-db.internal' },
      { key: 'port', label: 'Port', placeholder: '5432' },
      { key: 'database', label: 'Database', placeholder: 'analytics' },
      { key: 'user', label: 'Username', placeholder: 'app' },
      { key: 'password', label: 'Password', secret: true },
      { key: 'sslmode', label: 'SSL mode', optional: true, placeholder: 'disable' },
    ],
  },
  {
    type: 'mysql', label: 'MySQL — JDBC', family: 'db', desc: 'jdbc · mysql',
    fields: [
      { key: 'host', label: 'Host', placeholder: 'prod-db.internal' },
      { key: 'port', label: 'Port', placeholder: '3306' },
      { key: 'database', label: 'Database', placeholder: 'analytics' },
      { key: 'user', label: 'Username', placeholder: 'app' },
      { key: 'password', label: 'Password', secret: true },
    ],
  },
  {
    type: 'sqlite', label: 'SQLite — JDBC', family: 'db', desc: 'jdbc · sqlite',
    fields: [{ key: 'file', label: 'File path', placeholder: './data.db' }],
  },
  {
    type: 'kafka', label: 'Apache Kafka', family: 'stream', desc: 'kafka',
    fields: [
      { key: 'brokers', label: 'Bootstrap brokers', placeholder: 'broker-1:9092, broker-2:9092' },
      { key: 'securityProtocol', label: 'Security protocol', optional: true, placeholder: 'SASL_SSL' },
      { key: 'saslMechanism', label: 'SASL mechanism', optional: true, placeholder: 'SCRAM-SHA-512' },
      { key: 'user', label: 'Username', optional: true },
      { key: 'password', label: 'Password', secret: true, optional: true },
    ],
  },
  {
    type: 's3', label: 'Amazon S3', family: 'obj', desc: 'objstore · s3',
    fields: [
      { key: 'bucket', label: 'Bucket', placeholder: 'acme-lake' },
      { key: 'region', label: 'Region', placeholder: 'us-east-1' },
      { key: 'accessKeyId', label: 'Access key ID' },
      { key: 'secretAccessKey', label: 'Secret access key', secret: true },
      { key: 'endpoint', label: 'Endpoint', optional: true, placeholder: 'https://s3.amazonaws.com' },
    ],
  },
  {
    type: 'gcs', label: 'Google Cloud Storage', family: 'obj', desc: 'objstore · gcs',
    fields: [
      { key: 'bucket', label: 'Bucket', placeholder: 'acme-lake' },
      { key: 'projectId', label: 'Project ID', placeholder: 'acme-prod' },
      { key: 'serviceAccountJson', label: 'Service account JSON', secret: true, optional: true },
      { key: 'endpoint', label: 'Endpoint', optional: true, placeholder: 'http://127.0.0.1:4443/storage/v1/ (emulator)' },
    ],
  },
  {
    type: 'adls', label: 'Azure ADLS Gen2', family: 'obj', desc: 'objstore · adls',
    fields: [
      { key: 'account', label: 'Account', placeholder: 'acmestorage' },
      { key: 'container', label: 'Container', placeholder: 'lake' },
      { key: 'sasToken', label: 'SAS token / key', secret: true },
      { key: 'serviceURL', label: 'Service URL', optional: true, placeholder: 'http://127.0.0.1:10000/devstoreaccount1 (Azurite)' },
    ],
  },
  {
    type: 'http', label: 'HTTP endpoint — lookup enrichment', family: 'http', desc: 'http',
    fields: [
      { key: 'baseUrl', label: 'Base URL', placeholder: 'https://api.internal' },
      { key: 'authHeader', label: 'Auth header', secret: true, optional: true },
    ],
  },
];

export function connType(type: string): ConnType | undefined {
  return CONN_TYPES.find((c) => c.type === type);
}

// Map a source/sink stage type to a connection family so the inspector can offer
// only compatible connections.
export function familyOfStageType(type: string): ConnFamily | null {
  if (['jdbc', 'postgres', 'mysql', 'sqlite'].includes(type)) return 'db';
  if (type === 'kafka') return 'stream';
  if (['s3', 'gcs', 'adls', 'objstore'].includes(type)) return 'obj';
  if (type === 'http') return 'http';
  return null;
}

export function familyName(fam: ConnFamily): string {
  return ({ db: 'database', stream: 'Kafka', obj: 'object-store', http: 'HTTP' } as const)[fam] || fam;
}

// Stored (non-secret) shape.
export interface ConnectionMeta {
  id: string;
  name: string;
  type: string;
  family: ConnFamily;
  fields: Record<string, string>; // non-secret field values only
  secretKeys: string[]; // which field keys are stored in Secret Storage
}

// Lightweight shape sent to the webview (never includes secret values).
export interface ConnectionSummary {
  id: string;
  name: string;
  type: string;
  family: ConnFamily;
  desc: string;
}

export class ConnectionStore {
  private readonly emitter = new vscode.EventEmitter<void>();
  readonly onDidChange = this.emitter.event;
  private cache: ConnectionMeta[] | null = null;
  private readonly watcher: vscode.FileSystemWatcher;

  constructor(private readonly secrets: vscode.SecretStorage) {
    // Connections live as visible `connections/*.fgconn` files in the project —
    // watch them so external edits / git pulls refresh the tree.
    this.watcher = vscode.workspace.createFileSystemWatcher('**/connections/*.fgconn');
    const invalidate = () => {
      this.cache = null;
      this.emitter.fire();
    };
    this.watcher.onDidCreate(invalidate);
    this.watcher.onDidChange(invalidate);
    this.watcher.onDidDelete(invalidate);
  }

  dispose(): void {
    this.emitter.dispose();
    this.watcher.dispose();
  }

  // The `connections/` folder in the first workspace folder.
  private connectionsDir(): vscode.Uri | null {
    const folder = vscode.workspace.workspaceFolders?.[0];
    if (!folder) return null;
    return vscode.Uri.joinPath(folder.uri, 'connections');
  }

  private fileUriFor(id: string): vscode.Uri | null {
    const dir = this.connectionsDir();
    return dir ? vscode.Uri.joinPath(dir, `${id}.fgconn`) : null;
  }

  private secretKey(id: string, field: string): string {
    return `flogoDataPipeline.connection.${id}.${field}`;
  }

  async list(): Promise<ConnectionMeta[]> {
    if (this.cache) return this.cache;
    const dir = this.connectionsDir();
    if (!dir) return (this.cache = []);
    const out: ConnectionMeta[] = [];
    try {
      const entries = await vscode.workspace.fs.readDirectory(dir);
      for (const [name, kind] of entries) {
        if (kind !== vscode.FileType.File || !name.endsWith('.fgconn')) continue;
        try {
          const bytes = await vscode.workspace.fs.readFile(vscode.Uri.joinPath(dir, name));
          const parsed = JSON.parse(Buffer.from(bytes).toString('utf8')) as Partial<ConnectionMeta>;
          const id = parsed.id || name.replace(/\.fgconn$/, '');
          if (!parsed.type || !parsed.family) continue;
          out.push({
            id,
            name: parsed.name || id,
            type: parsed.type,
            family: parsed.family,
            fields: parsed.fields || {},
            secretKeys: Array.isArray(parsed.secretKeys) ? parsed.secretKeys : [],
          });
        } catch {
          /* skip malformed .fgconn */
        }
      }
    } catch {
      /* no connections/ dir yet */
    }
    // One-time migration from the legacy single-file store.
    if (!out.length) {
      const migrated = await this.migrateLegacy();
      if (migrated.length) {
        this.cache = migrated;
        return migrated;
      }
    }
    out.sort((a, b) => a.name.localeCompare(b.name));
    this.cache = out;
    return out;
  }

  // Import connections from the old `.flogo/connections.json` into per-file
  // `.fgconn` files (secrets already live in Secret Storage under the same id).
  private async migrateLegacy(): Promise<ConnectionMeta[]> {
    const folder = vscode.workspace.workspaceFolders?.[0];
    if (!folder) return [];
    const legacy = vscode.Uri.joinPath(folder.uri, '.flogo', 'connections.json');
    let list: ConnectionMeta[] = [];
    try {
      const bytes = await vscode.workspace.fs.readFile(legacy);
      const parsed = JSON.parse(Buffer.from(bytes).toString('utf8')) as { connections?: ConnectionMeta[] };
      list = Array.isArray(parsed.connections) ? parsed.connections : [];
    } catch {
      return [];
    }
    for (const record of list) {
      try {
        await this.writeFile(record);
      } catch {
        /* best effort */
      }
    }
    return list;
  }

  async summaries(): Promise<ConnectionSummary[]> {
    const list = await this.list();
    return list.map((c) => ({
      id: c.id,
      name: c.name,
      type: c.type,
      family: c.family,
      desc: connType(c.type)?.desc || c.type,
    }));
  }

  async get(id: string): Promise<ConnectionMeta | undefined> {
    return (await this.list()).find((c) => c.id === id);
  }

  async getByName(name: string): Promise<ConnectionMeta | undefined> {
    return (await this.list()).find((c) => c.name === name);
  }

  // Path (for messages) of a connection's .fgconn file.
  fsPathFor(id: string): string | null {
    return this.fileUriFor(id)?.fsPath ?? null;
  }

  // Write a single connection to its own `connections/<id>.fgconn` file. The
  // file is JSON WITHOUT any secret values — those go to Secret Storage and are
  // referenced by `secretKeys`.
  private async writeFile(record: ConnectionMeta): Promise<void> {
    const dir = this.connectionsDir();
    const uri = this.fileUriFor(record.id);
    if (!dir || !uri) throw new Error('Open a workspace folder to save connections.');
    try {
      await vscode.workspace.fs.createDirectory(dir);
    } catch {
      /* already exists */
    }
    const body = {
      // Friendly, self-documenting file — safe to commit (no secrets).
      version: 1,
      id: record.id,
      name: record.name,
      type: record.type,
      family: record.family,
      fields: record.fields,
      secretKeys: record.secretKeys,
      _note: 'Flogo Data Pipeline connection. Secrets are NOT stored here — they live in VS Code Secret Storage and are referenced by secretKeys.',
    };
    await vscode.workspace.fs.writeFile(uri, Buffer.from(JSON.stringify(body, null, 2), 'utf8'));
    this.cache = null;
    this.emitter.fire();
  }

  // Create or update a connection. `values` holds ALL entered field values
  // (secret + non-secret); secrets are split out into Secret Storage.
  async upsert(
    meta: { id?: string; name: string; type: string; family: ConnFamily },
    values: Record<string, string>,
  ): Promise<ConnectionMeta> {
    const ct = connType(meta.type);
    const secretFields = new Set((ct?.fields || []).filter((f) => f.secret).map((f) => f.key));
    const id = meta.id || meta.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') || 'conn';

    const nonSecret: Record<string, string> = {};
    const secretKeys: string[] = [];
    for (const [k, v] of Object.entries(values)) {
      if (v == null || v === '') continue;
      if (secretFields.has(k)) {
        await this.secrets.store(this.secretKey(id, k), v);
        secretKeys.push(k);
      } else {
        nonSecret[k] = v;
      }
    }

    const record: ConnectionMeta = {
      id,
      name: meta.name,
      type: meta.type,
      family: meta.family,
      fields: nonSecret,
      secretKeys,
    };

    await this.writeFile(record);
    return record;
  }

  async delete(id: string): Promise<void> {
    const record = await this.get(id);
    if (record) {
      for (const key of record.secretKeys) {
        await this.secrets.delete(this.secretKey(id, key));
      }
    }
    const uri = this.fileUriFor(id);
    if (uri) {
      try {
        await vscode.workspace.fs.delete(uri);
      } catch {
        /* already gone */
      }
    }
    this.cache = null;
    this.emitter.fire();
  }

  // Read a single secret value back (used for editing / resolving).
  async secret(id: string, field: string): Promise<string | undefined> {
    return this.secrets.get(this.secretKey(id, field));
  }

  // Return every field value (non-secret + secret) — for editing an existing
  // connection or resolving it at run time.
  async resolveValues(id: string): Promise<Record<string, string>> {
    const record = await this.get(id);
    if (!record) return {};
    const out: Record<string, string> = { ...record.fields };
    for (const key of record.secretKeys) {
      const v = await this.secret(id, key);
      if (v != null) out[key] = v;
    }
    return out;
  }

  // Build the concrete engine config a bound source/sink needs from a
  // connection, e.g. a Postgres connection → { driver, dsn }. Returns a partial
  // config object to merge into the stage.
  async resolveStageConfig(id: string): Promise<Record<string, unknown>> {
    const record = await this.get(id);
    if (!record) return {};
    const v = await this.resolveValues(id);
    return buildStageConfig(record.type, v);
  }

  // Resolve a connection for DEPLOYMENT: non-secret fields are inlined, but each
  // secret field value is replaced by a `${ENV_VAR}` placeholder that the engine
  // expands at runtime. Returns the stage config plus the list of environment
  // variables (name + real value) the deployment must supply. This keeps
  // secrets out of the generated .datapipeline / Dockerfile while producing a
  // runnable artifact.
  async resolveStageConfigForDeploy(
    id: string,
  ): Promise<{ config: Record<string, unknown>; env: Array<{ name: string; value: string }> }> {
    const record = await this.get(id);
    if (!record) return { config: {}, env: [] };
    const real = await this.resolveValues(id);
    const env: Array<{ name: string; value: string }> = [];
    const placeholders: Record<string, string> = { ...real };
    for (const key of record.secretKeys) {
      if (real[key] == null || real[key] === '') continue;
      const envName = envVarName(record.id, key);
      env.push({ name: envName, value: real[key] });
      placeholders[key] = '${' + envName + '}';
    }
    return { config: buildStageConfig(record.type, placeholders), env };
  }
}

// Build a source/sink stage config from a connection type + field values.
// Shared by resolveStageConfig (real values) and resolveStageConfigForDeploy
// (secret values replaced by ${ENV} placeholders).
function buildStageConfig(type: string, v: Record<string, string>): Record<string, unknown> {
  switch (type) {
    case 'postgres': {
      const parts = [
        v.host && `host=${v.host}`,
        v.port && `port=${v.port}`,
        v.database && `dbname=${v.database}`,
        v.user && `user=${v.user}`,
        v.password && `password=${v.password}`,
        `sslmode=${v.sslmode || 'disable'}`,
      ].filter(Boolean);
      return { driver: 'postgres', dsn: parts.join(' ') };
    }
    case 'mysql': {
      const auth = v.user ? `${v.user}${v.password ? ':' + v.password : ''}@` : '';
      const dsn = `${auth}tcp(${v.host}:${v.port || '3306'})/${v.database || ''}`;
      return { driver: 'mysql', dsn };
    }
    case 'sqlite':
      return { driver: 'sqlite', dsn: v.file || '' };
    case 'kafka': {
      const cfg: Record<string, unknown> = { brokers: v.brokers || '' };
      if (v.securityProtocol) cfg.securityProtocol = v.securityProtocol;
      if (v.saslMechanism) cfg.saslMechanism = v.saslMechanism;
      if (v.user) cfg.saslUser = v.user;
      if (v.password) cfg.saslPassword = v.password;
      return cfg;
    }
    case 's3':
    case 'gcs':
    case 'adls': {
      const cfg: Record<string, unknown> = {};
      if (v.bucket) cfg.bucket = v.bucket;
      if (v.region) cfg.region = v.region;
      if (v.endpoint) cfg.endpoint = v.endpoint;
      // S3-compatible endpoints (MinIO, LocalStack, Ceph) require path-style
      // addressing: the virtual-host style "<bucket>.<endpoint>" does not resolve
      // for a custom endpoint, so ListObjectsV2/GetObject fail with StatusCode 0.
      // Any explicit endpoint means a non-AWS S3, so force path-style there.
      if (type === 's3' && v.endpoint) cfg.forcePathStyle = true;
      if (v.accessKeyId) cfg.accessKeyId = v.accessKeyId;
      if (v.secretAccessKey) cfg.secretAccessKey = v.secretAccessKey;
      if (v.projectId) cfg.project = v.projectId;
      if (v.serviceAccountJson) cfg.credentialsJson = v.serviceAccountJson;
      // ADLS: engine reads accountName + serviceURL + sasToken/accountKey.
      if (v.account) cfg.accountName = v.account;
      if (v.container) cfg.container = v.container;
      if (v.sasToken) cfg.sasToken = v.sasToken;
      if (v.serviceURL) cfg.serviceURL = v.serviceURL;
      return cfg;
    }
    case 'http':
      return { baseUrl: v.baseUrl || '', ...(v.authHeader ? { authHeader: v.authHeader } : {}) };
    default:
      return { ...v };
  }
}

// Derive a stable, shell-safe env var name from a connection id + field.
export function envVarName(connId: string, field: string): string {
  const norm = (s: string) => s.replace(/[^A-Za-z0-9]+/g, '_').replace(/^_+|_+$/g, '').toUpperCase();
  return `${norm(connId)}_${norm(field)}`;
}

// Human path shown in messages: the connections/ folder.
export function connectionsFilePath(): string | null {
  const folder = vscode.workspace.workspaceFolders?.[0];
  if (!folder) return null;
  return path.join(folder.uri.fsPath, 'connections');
}
