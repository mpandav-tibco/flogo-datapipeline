// Schema inference from delimited / JSON-lines sample files.
//
// Reads the header + a sample of data rows and guesses a column type per field,
// so users can populate a source/sink `columns` list with one click instead of
// typing every name and type. Purely local file reads — no engine required.

import * as vscode from 'vscode';
import * as path from 'path';

export interface InferredColumn {
  name: string;
  type: 'string' | 'int' | 'float' | 'bool' | 'date';
}

interface InferOptions {
  delimiter?: string;
  hasHeader?: boolean;
  sourceType?: string; // csv | tsv | jsonlines | ...
  sampleRows?: number;
}

const DATE_RE = /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?/;
const INT_RE = /^-?\d+$/;
const FLOAT_RE = /^-?\d*\.\d+$/;
const BOOL_RE = /^(true|false)$/i;

// Expand ${date}-style templates to today's values (best effort). Returns the
// path with templates substituted, unchanged otherwise.
function expandTemplates(p: string): string {
  const now = new Date();
  const y = String(now.getFullYear());
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return p
    .replace(/\$\{date\}/g, `${y}-${m}-${d}`)
    .replace(/\$\{year\}/g, y)
    .replace(/\$\{month\}/g, m)
    .replace(/\$\{day\}/g, d);
}

// Candidate absolute paths to try for a (possibly relative) sample path: the
// workspace root first (matching the engine's run-time working directory), then
// the pipeline file's own directory. Absolute paths are returned as-is.
function candidatePaths(pipelineFile: string, p: string): string[] {
  const expanded = expandTemplates(p);
  if (path.isAbsolute(expanded)) return [expanded];
  const out: string[] = [];
  const wsRoot = vscode.workspace.getWorkspaceFolder(vscode.Uri.file(pipelineFile))?.uri.fsPath
    ?? vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
  if (wsRoot) out.push(path.join(wsRoot, expanded));
  out.push(path.join(path.dirname(pipelineFile), expanded));
  return out;
}

function classify(value: string): InferredColumn['type'] {
  const v = value.trim();
  if (v === '') return 'string';
  if (INT_RE.test(v)) return 'int';
  if (FLOAT_RE.test(v)) return 'float';
  if (BOOL_RE.test(v)) return 'bool';
  if (DATE_RE.test(v)) return 'date';
  return 'string';
}

// Combine two type guesses into the widest compatible type.
function widen(a: InferredColumn['type'], b: InferredColumn['type']): InferredColumn['type'] {
  if (a === b) return a;
  // int + float → float
  if ((a === 'int' && b === 'float') || (a === 'float' && b === 'int')) return 'float';
  // anything mixed → string
  return 'string';
}

function splitLine(line: string, delimiter: string): string[] {
  // Minimal CSV split with double-quote support (handles quoted delimiters).
  const out: string[] = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; } else inQ = false;
      } else cur += ch;
    } else if (ch === '"') {
      inQ = true;
    } else if (ch === delimiter) {
      out.push(cur);
      cur = '';
    } else cur += ch;
  }
  out.push(cur);
  return out;
}

export async function inferSchema(
  pipelineFile: string,
  rawPath: string,
  opts: InferOptions = {},
): Promise<InferredColumn[]> {
  const candidates = candidatePaths(pipelineFile, rawPath);
  let text: string | undefined;
  let target = candidates[0];
  for (const cand of candidates) {
    try {
      const bytes = await vscode.workspace.fs.readFile(vscode.Uri.file(cand));
      text = Buffer.from(bytes).toString('utf8');
      target = cand;
      break;
    } catch {
      /* try next candidate */
    }
  }
  if (text === undefined) {
    throw new Error(`Could not read sample file. Tried: ${candidates.join(' , ')}`);
  }

  const lines = text.split(/\r?\n/).filter((l) => l.length > 0);
  if (!lines.length) throw new Error('Sample file is empty.');

  const sourceType = (opts.sourceType || '').toLowerCase();
  const sampleRows = opts.sampleRows ?? 200;

  // JSON-lines: infer from keys across sampled objects.
  if (sourceType === 'jsonlines' || target.endsWith('.jsonl') || target.endsWith('.ndjson')) {
    const names: string[] = [];
    const types = new Map<string, InferredColumn['type']>();
    for (const line of lines.slice(0, sampleRows)) {
      let obj: any;
      try {
        obj = JSON.parse(line);
      } catch {
        continue;
      }
      if (!obj || typeof obj !== 'object') continue;
      for (const [k, v] of Object.entries(obj)) {
        if (!names.includes(k)) names.push(k);
        const t: InferredColumn['type'] =
          typeof v === 'number' ? (Number.isInteger(v) ? 'int' : 'float')
          : typeof v === 'boolean' ? 'bool'
          : typeof v === 'string' && DATE_RE.test(v) ? 'date'
          : 'string';
        types.set(k, types.has(k) ? widen(types.get(k)!, t) : t);
      }
    }
    if (!names.length) throw new Error('No JSON objects found in sample.');
    return names.map((n) => ({ name: n, type: types.get(n) || 'string' }));
  }

  // Delimited (csv/tsv/…).
  const delimiter = opts.delimiter || (sourceType === 'tsv' || target.endsWith('.tsv') ? '\t' : ',');
  const hasHeader = opts.hasHeader !== false;
  const headerCells = splitLine(lines[0], delimiter);
  const names = hasHeader
    ? headerCells.map((h, i) => h.trim() || `col_${i + 1}`)
    : headerCells.map((_, i) => `col_${i + 1}`);

  const dataStart = hasHeader ? 1 : 0;
  const colTypes: (InferredColumn['type'] | null)[] = names.map(() => null);
  for (let r = dataStart; r < Math.min(lines.length, dataStart + sampleRows); r++) {
    const cells = splitLine(lines[r], delimiter);
    for (let c = 0; c < names.length; c++) {
      const cell = cells[c];
      if (cell == null || cell.trim() === '') continue;
      const t = classify(cell);
      colTypes[c] = colTypes[c] == null ? t : widen(colTypes[c]!, t);
    }
  }
  return names.map((n, i) => ({ name: n, type: colTypes[i] || 'string' }));
}
