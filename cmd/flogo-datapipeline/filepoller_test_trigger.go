//go:build filepollertest

// This file registers the local, test-only File Poller trigger copy into the
// engine binary. It is guarded by the `filepollertest` build tag so the normal
// production binary (built without tags) does NOT include it. The File Poller's
// upstream module (github.com/tibco/flogo-files) is not accessible, so this
// vendored copy is the only way to validate it locally. Build the validation
// binary with:
//
//	go build -tags filepollertest -o flogo-datapipeline-fptest ./cmd/flogo-datapipeline/
package main

import (
	_ "github.com/mpandav-tibco/flogo-datapipeline/internal/testtrigger/filepoller"
)
