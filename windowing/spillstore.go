package windowing

import (
	"container/list"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"path/filepath"
)

// spillStore is a bounded-memory stateStore. Up to maxResident hot keys are kept
// in memory (LRU); colder keys are paged out to one small file per key, keeping
// only a compact in-memory key index resident. It trades speed for a bounded
// heap so a single instance can aggregate over high-cardinality key spaces
// without OOMing.
//
// Design note: get() deliberately does NOT promote a spilled key into memory, so
// a full Emit scan (which visits every key) does not pull the entire cold set
// into the heap. put() is what makes a key resident; in the windowed operator
// only keys whose windows survive an Emit are put back, so the resident set
// converges on the currently-active keys. The spill directory is ephemeral
// working state (removed on close) — durable resume goes through the operator
// Snapshot/Restore checkpoint, which visits spilled keys via keys()+get.
type spillStore struct {
	aggs        []AggSpec
	maxResident int
	dir         string

	hot  map[string]*keyState     // resident values (bounded by maxResident)
	lru  *list.List               // MRU at front; element value is the key string
	elem map[string]*list.Element // key -> its lru element
	disk map[string]struct{}      // keys whose value currently lives on disk
}

// newSpillStore creates a spillable store rooted at a fresh unique subdirectory
// of baseDir (or the OS temp dir when baseDir is empty). maxResident is clamped
// to at least 1.
func newSpillStore(aggs []AggSpec, maxResident int, baseDir string) (*spillStore, error) {
	if maxResident < 1 {
		maxResident = 1
	}
	if baseDir == "" {
		baseDir = os.TempDir()
	}
	dir, err := os.MkdirTemp(baseDir, "flogo-window-state-")
	if err != nil {
		return nil, err
	}
	return &spillStore{
		aggs:        aggs,
		maxResident: maxResident,
		dir:         dir,
		hot:         make(map[string]*keyState),
		lru:         list.New(),
		elem:        make(map[string]*list.Element),
		disk:        make(map[string]struct{}),
	}, nil
}

// path returns the on-disk file for a key, sharded into 256 subdirectories by
// the first byte of the key hash to keep any single directory small.
func (s *spillStore) path(key string) string {
	sum := sha1.Sum([]byte(key))
	name := hex.EncodeToString(sum[:])
	return filepath.Join(s.dir, name[:2], name+".ks")
}

func (s *spillStore) get(key string) (*keyState, bool) {
	if ks, ok := s.hot[key]; ok {
		s.touch(key)
		return ks, true
	}
	if _, ok := s.disk[key]; ok {
		data, err := os.ReadFile(s.path(key))
		if err != nil {
			return nil, false
		}
		ks, err := decodeKeyState(data, s.aggs)
		if err != nil {
			return nil, false
		}
		return ks, true // transient copy — intentionally NOT promoted to hot
	}
	return nil, false
}

func (s *spillStore) put(key string, ks *keyState) {
	// The key is now authoritative in memory; drop any stale on-disk copy.
	if _, ok := s.disk[key]; ok {
		delete(s.disk, key)
		_ = os.Remove(s.path(key))
	}
	if _, ok := s.hot[key]; ok {
		s.hot[key] = ks
		s.touch(key)
	} else {
		s.hot[key] = ks
		s.elem[key] = s.lru.PushFront(key)
	}
	s.evictIfNeeded()
}

func (s *spillStore) remove(key string) {
	if _, ok := s.hot[key]; ok {
		delete(s.hot, key)
		if e, ok := s.elem[key]; ok {
			s.lru.Remove(e)
			delete(s.elem, key)
		}
	}
	if _, ok := s.disk[key]; ok {
		delete(s.disk, key)
		_ = os.Remove(s.path(key))
	}
}

func (s *spillStore) keys() []string {
	out := make([]string, 0, len(s.hot)+len(s.disk))
	for k := range s.hot {
		out = append(out, k)
	}
	for k := range s.disk {
		out = append(out, k)
	}
	return out
}

func (s *spillStore) len() int { return len(s.hot) + len(s.disk) }

func (s *spillStore) clear() {
	s.hot = make(map[string]*keyState)
	s.lru = list.New()
	s.elem = make(map[string]*list.Element)
	for k := range s.disk {
		_ = os.Remove(s.path(k))
	}
	s.disk = make(map[string]struct{})
}

func (s *spillStore) close() error { return os.RemoveAll(s.dir) }

func (s *spillStore) touch(key string) {
	if e, ok := s.elem[key]; ok {
		s.lru.MoveToFront(e)
	}
}

// evictIfNeeded pages the least-recently-used resident keys out to disk until the
// resident set is within maxResident. A spill that fails to write drops the
// victim's partial aggregation rather than growing the heap — a memory-safety
// fallback (counted as an eviction by the operator if wired).
func (s *spillStore) evictIfNeeded() {
	for len(s.hot) > s.maxResident {
		back := s.lru.Back()
		if back == nil {
			return
		}
		vk := back.Value.(string)
		s.lru.Remove(back)
		delete(s.elem, vk)
		victim := s.hot[vk]
		delete(s.hot, vk)
		if data, err := encodeKeyState(victim); err == nil {
			p := s.path(vk)
			if os.MkdirAll(filepath.Dir(p), 0o755) == nil && atomicWrite(p, data) == nil {
				s.disk[vk] = struct{}{}
			}
		}
	}
}

// atomicWrite writes data to path via a temp file + rename so a crash never
// leaves a partially written key file.
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
