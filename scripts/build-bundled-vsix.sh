#!/usr/bin/env bash
# Build "batteries-included" per-platform VSIXs for Flogo Data Pipeline Studio.
#
# Each VSIX bundles:
#   • the extension JS (dist/)
#   • a prebuilt MONOLITH engine  → Run / Preview (no toolchain needed)
#   • a prebuilt dp-build helper   → lean Deploy build
#   • the vendored module source (media/dp-src) → the lean build compiles a
#     per-app binary offline, with no repo/source access (the OOTB Flogo model).
#
# Usage:
#   scripts/build-bundled-vsix.sh                       # all 6 targets
#   scripts/build-bundled-vsix.sh darwin-arm64 linux-x64  # a subset
#
# Env overrides:
#   FLOGO_GENERAL_SRC  path to the private TIBCO "General" module source
#                      (ships inside the TIBCO Flogo VS Code extension)
#   GO                 go binary (default: go on PATH)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXT="$REPO_ROOT/studio/vscode"
BIN_STAGE="$REPO_ROOT/.dpbin"          # all compiled binaries (gitignored)
DPSRC="$EXT/media/dp-src"              # bundled offline build tree (gitignored)
OUT="$EXT/dist-vsix"                   # emitted VSIXs (gitignored)
GO="${GO:-go}"
FLOGO_GENERAL_SRC="${FLOGO_GENERAL_SRC:-$HOME/.vscode/extensions/tibco.flogo-2.26.5-2893/media/flogo-contributions/wistudio/v1/contributions/Tibco/General}"

# target triples: "GOOS GOARCH vsce-target"
declare -a ALL=(
  "darwin  arm64 darwin-arm64"
  "darwin  amd64 darwin-x64"
  "linux   amd64 linux-x64"
  "linux   arm64 linux-arm64"
  "windows amd64 win32-x64"
  "windows arm64 win32-arm64"
)
# Select requested targets (by vsce-target name), or all.
declare -a TARGETS=()
if [ "$#" -gt 0 ]; then
  for want in "$@"; do
    for row in "${ALL[@]}"; do
      # shellcheck disable=SC2086
      set -- $row; [ "$3" = "$want" ] && TARGETS+=("$row")
    done
  done
else
  TARGETS=("${ALL[@]}")
fi
[ "${#TARGETS[@]}" -eq 0 ] && { echo "no matching targets"; exit 2; }

VERSION="$(node -p "require('$EXT/package.json').version")"
echo "== Flogo Data Pipeline Studio bundled VSIX build — v$VERSION =="

# ── restore tracked go.mod/go.sum on exit (the bundle uses a relative replace) ─
cd "$REPO_ROOT"
cp go.mod /tmp/dp.go.mod.bak
cp go.sum /tmp/dp.go.sum.bak 2>/dev/null || true
cleanup() { cp /tmp/dp.go.mod.bak "$REPO_ROOT/go.mod"; [ -f /tmp/dp.go.sum.bak ] && cp /tmp/dp.go.sum.bak "$REPO_ROOT/go.sum"; }
trap cleanup EXIT

# ── 1. self-contained source: bundle flogo-general + vendor all deps ──────────
echo "-- vendoring self-contained build tree --"
[ -d "$FLOGO_GENERAL_SRC" ] || { echo "flogo-general source not found: $FLOGO_GENERAL_SRC" >&2; exit 1; }
rm -rf third_party/flogo-general/General
mkdir -p third_party/flogo-general
cp -R "$FLOGO_GENERAL_SRC" third_party/flogo-general/General
"$GO" mod edit -replace github.com/tibco/flogo-general/src/app/General=./third_party/flogo-general/General
"$GO" mod tidy >/dev/null 2>&1 || true
"$GO" mod vendor

# ── 2. stage media/dp-src (source + vendor + flogo-general + go.mod) ───────────
echo "-- staging media/dp-src --"
rm -rf "$DPSRC"; mkdir -p "$DPSRC"
for d in action api checkpoint internal metrics objstore partition record \
         scaling schema sdk sink source transform watermark build \
         vendor third_party; do
  [ -e "$d" ] && cp -R "$d" "$DPSRC/"
done
cp go.mod go.sum "$DPSRC/"
date -u +%Y%m%dT%H%M%SZ > "$DPSRC/.bundle-version"
echo "   media/dp-src = $(du -sh "$DPSRC" | cut -f1)"

# ── 3. cross-compile prebuilt binaries (CGO-free) for each requested target ───
echo "-- compiling engine + dp-build --"
rm -rf "$BIN_STAGE"; mkdir -p "$BIN_STAGE"
for row in "${TARGETS[@]}"; do
  # shellcheck disable=SC2086
  set -- $row; goos="$1"; goarch="$2"
  ext=""; [ "$goos" = "windows" ] && ext=".exe"
  echo "   $goos/$goarch"
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" "$GO" build -trimpath -ldflags "-s -w" -mod=vendor \
    -o "$BIN_STAGE/flogo-datapipeline-$goos-$goarch$ext" ./cmd/flogo-datapipeline/
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" "$GO" build -trimpath -ldflags "-s -w" -mod=vendor \
    -o "$BIN_STAGE/dp-build-$goos-$goarch$ext" ./cmd/dp-build/
done

# ── 4. package one VSIX per target (only that platform's 2 binaries in bin/) ───
echo "-- packaging VSIXs --"
mkdir -p "$OUT"
export PATH="/opt/homebrew/opt/node@20/bin:$PATH"   # vsce/undici need node >= 20
for row in "${TARGETS[@]}"; do
  # shellcheck disable=SC2086
  set -- $row; goos="$1"; goarch="$2"; vt="$3"
  ext=""; [ "$goos" = "windows" ] && ext=".exe"
  rm -rf "$EXT/bin"; mkdir -p "$EXT/bin"
  cp "$BIN_STAGE/flogo-datapipeline-$goos-$goarch$ext" "$EXT/bin/"
  cp "$BIN_STAGE/dp-build-$goos-$goarch$ext"           "$EXT/bin/"
  echo "   → $vt"
  ( cd "$EXT" && npx --yes @vscode/vsce@latest package --target "$vt" \
      -o "$OUT/flogo-datapipeline-studio-$vt-$VERSION.vsix" >/dev/null 2>&1 )
  echo "     $(du -h "$OUT/flogo-datapipeline-studio-$vt-$VERSION.vsix" | cut -f1)"
done
rm -rf "$EXT/bin"

echo "== done → $OUT =="
ls -la "$OUT"
