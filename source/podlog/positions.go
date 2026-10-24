package podlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/fileutil"
)

// position records how far a single log file has been durably delivered.
//
// Identity is deliberately OS-portable: rather than depend on device/inode
// numbers (which require platform-specific syscalls and do not compile on
// Windows dev machines), a file is keyed by its path and its rotation is
// detected via a content Fingerprint of the first bytes plus size regression.
// containerd/CRI-O rotate by renaming the active file and creating a fresh one
// at the same path, so a changed fingerprint or a size smaller than the stored
// offset unambiguously signals a new file whose offset must reset to zero.
type position struct {
	Offset      int64  `json:"offset"`
	Fingerprint string `json:"fingerprint"`
}

// positionStore is the authoritative, crash-safe record of delivery progress —
// the equivalent of Fluentd's pos file or Filebeat's registry. It is persisted
// only after the sink has acknowledged the corresponding records, which is what
// makes the pipeline at-least-once: a crash replays from the last persisted
// offset, producing safe duplicates rather than silent loss.
type positionStore struct {
	path string
	mu   sync.Mutex
	m    map[string]position
	// existed reports that a registry was found at startup (not the very
	// first run of this collector on the node).
	existed bool
}

func newPositionStore(path string) (*positionStore, error) {
	ps := &positionStore{path: path, m: map[string]position{}}
	if path == "" {
		return ps, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ps, nil
		}
		return nil, err
	}
	ps.existed = true
	if len(data) > 0 {
		if err := json.Unmarshal(data, &ps.m); err != nil {
			// An unreadable registry must not stop collection on the node
			// (the run would fail and restart forever). Keep it for analysis
			// and start over: with existed=true every file is read from its
			// start — duplicates, never loss.
			bad := fmt.Sprintf("%s.corrupt-%d", path, time.Now().Unix())
			_ = os.Rename(path, bad)
			plog.Errorf("podlog source: position registry %s is unreadable (%v); moved to %s, re-reading all log files from the start (duplicates possible)", path, err, bad)
			ps.m = map[string]position{}
		}
	}
	return ps, nil
}

// gc drops the positions of files whose container log directory no longer
// exists (the pod was removed), so the registry — rewritten at every
// checkpoint — does not grow with every pod ever seen on the node. It returns
// the removed keys.
func (ps *positionStore) gc() []string {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	gone := map[string]bool{}
	var removed []string
	for key := range ps.m {
		dir := filepath.Dir(key)
		isGone, checked := gone[dir]
		if !checked {
			_, err := os.Stat(dir)
			isGone = os.IsNotExist(err)
			gone[dir] = isGone
		}
		if isGone {
			delete(ps.m, key)
			removed = append(removed, key)
		}
	}
	return removed
}

func (ps *positionStore) get(key string) (position, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	p, ok := ps.m[key]
	return p, ok
}

func (ps *positionStore) set(key string, p position) {
	ps.mu.Lock()
	ps.m[key] = p
	ps.mu.Unlock()
}

// snapshot returns a JSON copy of all positions for the pipeline checkpoint
// layer (Token.Payload) — used for observability/audit, not for resume.
func (ps *positionStore) snapshot() json.RawMessage {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	b, _ := json.Marshal(ps.m)
	return json.RawMessage(b)
}

// persist writes the position map atomically AND durably (fsync temp + rename +
// fsync dir, via fileutil.WriteFileAtomic) so a crash or power loss mid-write
// cannot corrupt or leave a stale registry — the crash-safe contract above.
func (ps *positionStore) persist() error {
	if ps.path == "" {
		return nil
	}
	ps.mu.Lock()
	b, err := json.Marshal(ps.m)
	ps.mu.Unlock()
	if err != nil {
		return err
	}
	return fileutil.WriteFileAtomic(ps.path, b, 0o644)
}
