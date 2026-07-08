// Package file registers a file-based checkpoint backend.
// Checkpoint tokens are stored as JSON files in a configurable directory.
// Suitable for single-instance pipelines; use the Redis backend for
// multi-instance deployments where pods may be rescheduled.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/checkpoint/file"
package file

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
)

func init() {
	checkpoint.Register("file", newFileCheckpoint)
}

type fileCheckpoint struct {
	dir string
	mu  sync.Mutex
}

func newFileCheckpoint(cfg map[string]interface{}) (checkpoint.Checkpoint, error) {
	dir, _ := cfg["path"].(string)
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("file checkpoint: create dir %q: %w", dir, err)
	}
	return &fileCheckpoint{dir: dir}, nil
}

func (f *fileCheckpoint) path(partitionID string) string {
	// Escape filesystem-unsafe characters so the full partition ID is
	// preserved in the filename without truncation or collision.
	// filepath.Base was previously used here but strips everything up to
	// the last path separator, causing 'region/us-east/shard-0' and
	// 'region/eu-west/shard-0' to both map to 'shard-0.ckpt.json'.
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
	).Replace(partitionID)
	return filepath.Join(f.dir, safe+".ckpt.json")
}

func (f *fileCheckpoint) Save(partitionID string, t *checkpoint.Token) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return fmt.Errorf("file checkpoint: marshal: %w", err)
	}
	// Write to temp file then rename for atomicity.
	tmp := f.path(partitionID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("file checkpoint: write temp: %w", err)
	}
	return os.Rename(tmp, f.path(partitionID))
}

func (f *fileCheckpoint) Load(partitionID string) (*checkpoint.Token, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path(partitionID))
	if os.IsNotExist(err) {
		return nil, nil // no checkpoint — fresh start
	}
	if err != nil {
		return nil, fmt.Errorf("file checkpoint: read: %w", err)
	}
	var t checkpoint.Token
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("file checkpoint: parse: %w", err)
	}
	return &t, nil
}

func (f *fileCheckpoint) Delete(partitionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := os.Remove(f.path(partitionID))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (f *fileCheckpoint) Close() error { return nil }
