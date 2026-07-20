#!/usr/bin/env bash
#
# dev-reload.sh — one-command local dev loop (NO VSIX packaging/installing).
#
# Rebuilds:
#   1. the engine binary  (Go: podlog source, maxLines, shutdown-panic fix, …)
#   2. the Studio webview (TS: podlog form, labels, …)
# then syncs the built bundle into the installed extension dir(s) so a
# "Developer: Reload Window" in VS Code picks up the latest.
#
# Run after ANY engine or UI change:
#   studio/vscode/scripts/dev-reload.sh
# then in VS Code: Command Palette -> Developer: Reload Window
#
# (If you run the F5 "Run Data Pipeline Studio" dev host instead, you only need
#  step 1+2 and a Cmd+R in that window — the sync step just no-ops.)
set -euo pipefail

# Locate studio/vscode and the repo root regardless of where this is invoked
# (works under bash and zsh).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
SVDIR="$(cd "$SCRIPT_DIR/.." && pwd)"   # studio/vscode
ROOT="$(cd "$SVDIR/../.." && pwd)"      # repo root

echo "repo:   $ROOT"
echo "studio: $SVDIR"

# ── 1. engine binary — the one the Studio runs via binaryPath ─────────────────
echo
echo "== [1/3] building engine binary =="
( cd "$ROOT" && CGO_ENABLED=1 go build -o flogo-datapipeline ./cmd/flogo-datapipeline )
echo "  ok: $ROOT/flogo-datapipeline ($(cd "$ROOT" && ls -lh flogo-datapipeline | awk '{print $5, $6, $7, $8}'))"

# ── 2. Studio webview bundle ──────────────────────────────────────────────────
echo
echo "== [2/3] building Studio webview =="
( cd "$SVDIR" && NODE_OPTIONS=--max-old-space-size=4096 npm run compile )
echo "  ok: $SVDIR/dist/webview.js"

# ── 3. sync bundle into the installed extension dir(s) ────────────────────────
echo
echo "== [3/3] syncing into installed extension(s) =="
VERSION="$(node -p "require('$SVDIR/package.json').version")"
EXT_ID="tibco.flogo-datapipeline-studio-$VERSION"
synced=0
for base in "$HOME/.vscode/extensions" "$HOME/.cursor/extensions"; do
  dest="$base/$EXT_ID/dist"
  if [ -d "$dest" ]; then
    cp "$SVDIR"/dist/*.js "$dest"/ 2>/dev/null || true
    cp "$SVDIR"/dist/*.js.map "$dest"/ 2>/dev/null || true
    echo "  synced -> $dest"
    synced=$((synced + 1))
  fi
done
if [ "$synced" -eq 0 ]; then
  echo "  note: no installed extension dir found for $EXT_ID"
  echo "        (using the F5 dev host? then no sync is needed.)"
fi

echo
echo "done. In VS Code: run 'Developer: Reload Window' to load the latest UI."
echo "      Engine changes also apply to standalone runs of $ROOT/flogo-datapipeline."
