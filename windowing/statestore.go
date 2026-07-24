package windowing

// stateStore abstracts the operator's keyed window state so the same operator
// logic runs over either a plain in-memory map (default, fast path) or a
// spillable backend that keeps only hot keys resident and pages cold keys to an
// embedded on-disk store (bounded memory for high-cardinality key spaces).
//
// The operator drives it from a single goroutine, so implementations need not be
// safe for concurrent use. The canonical iteration pattern tolerates mutation of
// the store during a sweep:
//
//	for _, k := range store.keys() {   // keys() is a snapshot
//	    st, ok := store.get(k)
//	    if !ok { continue }
//	    // ...mutate st.windows...
//	    if len(st.windows) == 0 { store.remove(k) } else { store.put(k, st) }
//	}
//
// For an in-memory store get returns the live pointer, so the trailing put is a
// no-op; for a spillable store get returns a decoded value and put re-encodes it.
type stateStore interface {
	// get loads the keyState for key, or (nil,false) if absent.
	get(key string) (*keyState, bool)
	// put stores (or replaces) the keyState for key. For spillable stores this
	// is what actually persists in-place mutations made after get.
	put(key string, ks *keyState)
	// remove deletes the entry for key (no-op if absent).
	remove(key string)
	// keys returns a snapshot of all resident + spilled keys. Safe to iterate
	// while calling put/remove on the store.
	keys() []string
	// len reports the total number of keys held (resident + spilled).
	len() int
	// clear drops all state.
	clear()
	// close releases any backing resources (files, handles). Idempotent.
	close() error
}

// memStore is the default in-memory stateStore: a plain map with no spill. It is
// behaviour-identical to the operator's original raw map and adds no I/O.
type memStore struct {
	m map[string]*keyState
}

func newMemStore() *memStore { return &memStore{m: make(map[string]*keyState)} }

func (s *memStore) get(key string) (*keyState, bool) {
	ks, ok := s.m[key]
	return ks, ok
}

func (s *memStore) put(key string, ks *keyState) { s.m[key] = ks }

func (s *memStore) remove(key string) { delete(s.m, key) }

func (s *memStore) keys() []string {
	ks := make([]string, 0, len(s.m))
	for k := range s.m {
		ks = append(ks, k)
	}
	return ks
}

func (s *memStore) len() int { return len(s.m) }

func (s *memStore) clear() { s.m = make(map[string]*keyState) }

func (s *memStore) close() error { return nil }
