// Package file registers a file-backed watermark store.
// Marks are stored as JSON files in a configurable directory, one file per
// pipeline ID. Atomic writes use a temp-file-then-rename pattern identical to
// the file checkpoint backend, ensuring crash safety on POSIX file systems.
//
// Suitable for single-instance pipelines on shared or local storage.
// For multi-instance or Kubernetes deployments use the redis or postgres backend.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/watermark/file"
package file

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/fileutil"
	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

func init() {
	watermark.Register("file", newFileStore)
}

type fileStore struct {
	dir string
	mu  sync.Mutex
}

func newFileStore(cfg map[string]interface{}) (watermark.Store, error) {
	dir, _ := cfg["path"].(string)
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("file watermark: create dir %q: %w", dir, err)
	}
	return &fileStore{dir: dir}, nil
}

// path returns the file system path for a given pipelineID.
// Unsafe characters are percent-encoded so the full ID is preserved in the
// filename without truncation or collision — mirrors the file checkpoint logic.
func (f *fileStore) path(pipelineID string) string {
	safe := strings.NewReplacer(
		"/", "%2F",
		"\\", "%5C",
		":", "%3A",
		"*", "%2A",
		"?", "%3F",
		"\"", "%22",
		"<", "%3C",
		">", "%3E",
		"|", "%7C",
	).Replace(pipelineID)
	return filepath.Join(f.dir, safe+".hwm.json")
}

func (f *fileStore) Load(pipelineID string) (*watermark.Mark, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path(pipelineID))
	if os.IsNotExist(err) {
		return nil, nil // no mark — first run, full-table load
	}
	if err != nil {
		return nil, fmt.Errorf("file watermark: read %q: %w", pipelineID, err)
	}
	var m watermark.Mark
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("file watermark: parse %q: %w", pipelineID, err)
	}
	return &m, nil
}

func (f *fileStore) Save(pipelineID string, m *watermark.Mark) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("file watermark: marshal %q: %w", pipelineID, err)
	}
	// Atomic + durable write (temp file, fsync, rename, best-effort dir fsync):
	// atomic so a reader never sees a torn mark, durable so the mark survives a
	// power loss — otherwise the next run would do a full-table load.
	if err := fileutil.WriteFileAtomic(f.path(pipelineID), data, 0644); err != nil {
		return fmt.Errorf("file watermark: write %q: %w", pipelineID, err)
	}
	return nil
}

func (f *fileStore) Close() error { return nil }
