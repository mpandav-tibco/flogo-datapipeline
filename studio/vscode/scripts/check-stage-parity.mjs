#!/usr/bin/env node
// check-stage-parity.mjs — fails loudly the moment the engine registers a
// source/sink/transform/dq-expectation type the Studio's STAGE_CATALOG (or
// DQ_EXPECTATIONS) doesn't know about yet.
//
// This is the parity guardrail from the Studio architecture review: every one
// of the 6 backend-vs-UI gaps found in that review (avro/rest source+sink,
// broadcast transform, the referential-integrity dq expectation, and the
// entire `contract` feature) existed because the Studio's hand-maintained
// catalogs had no automated check tying them back to what the engine actually
// registers. This script IS that check.
//
// Ground truth: `flogo-datapipeline -describe` (cmd/flogo-datapipeline/main.go)
// prints every registered source/sink/transform type (source.Formats() /
// sink.Types() / transform.Types()) and dq expectation type
// (dq.ExpectationTypes()) as JSON — no pipeline file needed.
//
// Usage:
//   node scripts/check-stage-parity.mjs [path-to-engine-binary]
// Exit code 0 = no gaps. Exit code 1 = at least one backend type has no
// matching Studio entry (and is not a recognised alias — see STAGE_TYPE_ALIAS
// in src/model/stageCatalog.ts, which this script parses too).
//
// This is intentionally a lightweight regex-based check against the TS
// SOURCE (not a compiled import) of src/model/stageCatalog.ts, since a plain
// Node script has no TS loader configured here.

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STUDIO_DIR = path.resolve(__dirname, '..');
const REPO_ROOT = path.resolve(STUDIO_DIR, '..', '..');
const CATALOG_TS = path.join(STUDIO_DIR, 'src', 'model', 'stageCatalog.ts');

function findEngineBinary() {
    const arg = process.argv[2];
    if (arg) return arg;
    const candidates = [
        path.join(REPO_ROOT, 'flogo-datapipeline'),
        path.join(REPO_ROOT, 'flogo-datapipeline.exe'),
    ];
    for (const c of candidates) if (existsSync(c)) return c;
    return null;
}

function describeEngine(binPath) {
    const out = execFileSync(binPath, ['-describe'], { encoding: 'utf8' });
    return JSON.parse(out);
}

// Extracts every `kind: '<kind>', type: '<type>'` pair from the STAGE_CATALOG
// literal in src/model/stageCatalog.ts.
function extractCatalogTypes(src, kind) {
    const re = new RegExp(`kind: '${kind}', type: '([a-zA-Z0-9_-]+)'`, 'g');
    const out = new Set();
    let m;
    while ((m = re.exec(src))) out.add(m[1]);
    return out;
}

// Extracts the STAGE_TYPE_ALIAS map: { source: {...}, sink: {...}, transform: {...} }.
function extractAliases(src, kind) {
    const block = src.match(/const STAGE_TYPE_ALIAS[^{]*=\s*\{([\s\S]*?)\n\};/);
    if (!block) return new Set();
    const lineRe = new RegExp(`^\\s*${kind}:\\s*\\{(.*)\\},?$`, 'm');
    const line = block[1].match(lineRe);
    if (!line) return new Set();
    const out = new Set();
    const pairRe = /'?([a-zA-Z0-9_-]+)'?\s*:\s*'([a-zA-Z0-9_-]+)'/g;
    let m;
    while ((m = pairRe.exec(line[1]))) out.add(m[1]);
    return out;
}

// Extracts every `{ type: 'expectXxx'` entry from the DQ_EXPECTATIONS catalog.
function extractDQExpectations(src) {
    const block = src.match(/const DQ_EXPECTATIONS[^=]*=\s*\[([\s\S]*?)\n\];/);
    const scope = block ? block[1] : src;
    const re = /type: '(expect[a-zA-Z]+)'/g;
    const out = new Set();
    let m;
    while ((m = re.exec(scope))) out.add(m[1]);
    return out;
}

function checkKind(kind, described, src) {
    const catalog = extractCatalogTypes(src, kind);
    const aliases = extractAliases(src, kind);
    const excluded = KNOWN_EXCLUSIONS[kind] || new Set();
    const missing = described.filter((t) => !catalog.has(t) && !aliases.has(t) && !excluded.has(t));
    const extra = [...catalog].filter((t) => !described.includes(t));
    return { missing, extra, catalogCount: catalog.size, aliasCount: aliases.size };
}

// Registered types the Studio deliberately does NOT expose via the regular
// source/sink palette — not aliases (they're real, distinct backend types),
// just handled through a different UI mechanism. Keep this list short and
// justify every entry; anything else missing is a genuine gap.
const KNOWN_EXCLUSIONS = {
    // dlq is a sink type, but the Studio never offers it as a *main* sink pick —
    // it's the pipeline's separate error-routing destination, added/configured
    // through its own dedicated flow (addDlq()/DLQ_DEST_TYPES/defaultDlqOptions),
    // not the Sinks palette.
    sink: new Set(['dlq']),
};

function main() {
    const binPath = findEngineBinary();
    if (!binPath) {
        console.error('check-stage-parity: no engine binary found. Build it first:');
        console.error('  CGO_ENABLED=1 go build -o flogo-datapipeline ./cmd/flogo-datapipeline');
        console.error('or pass a path: node scripts/check-stage-parity.mjs <path-to-binary>');
        process.exit(1);
    }
    const manifest = describeEngine(binPath);
    const src = readFileSync(CATALOG_TS, 'utf8');

    let failed = false;
    for (const kind of ['source', 'sink', 'transform']) {
        const described = manifest[kind + 's'] || manifest[kind === 'source' ? 'sources' : kind === 'sink' ? 'sinks' : 'transforms'];
        const r = checkKind(kind, described, src);
        console.log(`${kind}: engine=${described.length} catalog=${r.catalogCount} aliases=${r.aliasCount}${r.extra.length ? ` extra=${JSON.stringify(r.extra)}` : ''}`);
        if (r.missing.length) {
            failed = true;
            console.error(`  ✗ MISSING from STAGE_CATALOG (and not a recognised STAGE_TYPE_ALIAS): ${JSON.stringify(r.missing)}`);
        }
    }

    const dqCatalog = extractDQExpectations(src);
    const dqMissing = (manifest.dqExpectations || []).filter((t) => !dqCatalog.has(t));
    console.log(`dq expectations: engine=${(manifest.dqExpectations || []).length} catalog=${dqCatalog.size}`);
    if (dqMissing.length) {
        failed = true;
        console.error(`  ✗ MISSING from DQ_EXPECTATIONS: ${JSON.stringify(dqMissing)}`);
    }

    if (failed) {
        console.error('\ncheck-stage-parity: FAILED — the engine registers a type the Studio does not know about.');
        console.error('Add it to STAGE_CATALOG / DQ_EXPECTATIONS (webview/main.ts), or to STAGE_TYPE_ALIAS if it is');
        console.error('a deliberate backward-compat synonym that should not get its own palette entry.');
        process.exit(1);
    }
    console.log('\ncheck-stage-parity: OK — every engine-registered type has a matching Studio entry.');
}

main();
