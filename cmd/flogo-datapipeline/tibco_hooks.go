//go:build tibco_hooks

// Package main — TIBCO General hooks (App Startup/App Shutdown triggers),
// isolated behind the "tibco_hooks" build tag.
//
// App Startup fires the pipeline once on engine boot (the correct driver for
// a long-running streaming source such as the podlog tailer); App Shutdown
// fires once on stop for final flush/cleanup.
//
// WHY THIS IS TAG-GATED (not a plain import in main.go): these two triggers
// live in github.com/tibco/flogo-general/src/app/General, a PRIVATE TIBCO
// module that is not published to any public Go module proxy — it ships only
// inside the TIBCO Flogo VS Code extension bundle. This repo's go.work
// resolves it via a local, git-ignored `replace` directive pointing at that
// extension's bundled source (see go.work's own comment), so it builds fine
// on a machine with that extension installed, but a plain `git clone` +
// `go build ./...` on any OTHER machine (including CI, and any contributor
// without the extension) fails outright: go.mod lists the module as a
// `require`, but there is no go.sum entry and no public source to resolve it
// from ("missing go.sum entry for module providing package ...").
//
// Gating the import behind this file + build tag keeps the DEFAULT build
// (no tags — what CI, `go build ./...`, and any external contributor gets)
// fully self-contained and buildable with zero private dependencies. Building
// the COMPLETE production binary, with these two OOTB triggers registered,
// additionally requires `-tags tibco_hooks` AND the local go.work replace
// (i.e. the TIBCO Flogo VS Code extension installed) — see docs/deployment.md.
package main

import (
	_ "github.com/tibco/flogo-general/src/app/General/trigger/shutdownhook"
	_ "github.com/tibco/flogo-general/src/app/General/trigger/startuphook"
)
