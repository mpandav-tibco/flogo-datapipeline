package windowing

import "container/heap"

// deadline schedules one (key, window) for firing at window End. The heap is
// an index over the state store, not a second source of truth: entries go
// stale when a window is merged away (sessions), evicted, or lost to a failed
// spill, and are simply skipped when popped if the window no longer exists.
type deadline struct {
	end int64
	key string
	w   Window
}

type deadlineHeap []deadline

func (h deadlineHeap) Len() int            { return len(h) }
func (h deadlineHeap) Less(i, j int) bool  { return h[i].end < h[j].end }
func (h deadlineHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *deadlineHeap) Push(x interface{}) { *h = append(*h, x.(deadline)) }
func (h *deadlineHeap) Pop() interface{} {
	old := *h
	n := len(old)
	d := old[n-1]
	old[n-1] = deadline{}
	*h = old[:n-1]
	return d
}

func (o *Operator) schedule(key string, w Window) {
	heap.Push(&o.due, deadline{end: w.End, key: key, w: w})
	// Stale entries (merged/evicted windows) accumulate until their End passes
	// the watermark. If they ever dominate the index, rebuild it from state.
	if len(o.due) > 4*o.openWindowCount+1024 {
		o.rebuildDeadlines()
	}
}

// rebuildDeadlines reconstructs the index from the state store. Used after
// Restore and as amortised compaction; it is the only full-state scan left on
// the steady-state path.
func (o *Operator) rebuildDeadlines() {
	o.due = o.due[:0]
	for _, key := range o.store.keys() {
		st, ok := o.store.get(key)
		if !ok {
			continue
		}
		for w := range st.windows {
			o.due = append(o.due, deadline{end: w.End, key: key, w: w})
		}
	}
	heap.Init(&o.due)
}

// popDue removes every window with End <= limit from state, calling fn for
// each one that still exists, and persists the touched keys. Keys are loaded
// from the store at most once per call.
func (o *Operator) popDue(limit int64, fn func(key string, st *keyState, w Window, accs []Accumulator)) {
	var touched map[string]*keyState
	for len(o.due) > 0 && o.due[0].end <= limit {
		d := heap.Pop(&o.due).(deadline)
		st, ok := touched[d.key]
		if !ok {
			if st, ok = o.store.get(d.key); !ok {
				continue
			}
			if touched == nil {
				touched = make(map[string]*keyState)
			}
			touched[d.key] = st
		}
		accs, ok := st.windows[d.w]
		if !ok {
			continue // stale: merged away or already removed
		}
		fn(d.key, st, d.w, accs)
		delete(st.windows, d.w)
		if st.earlyFired != nil {
			delete(st.earlyFired, d.w)
		}
		o.openWindowCount--
	}
	for key, st := range touched {
		if len(st.windows) == 0 {
			o.store.remove(key)
		} else {
			o.store.put(key, st)
		}
	}
}
