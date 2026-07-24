package windowing

import "encoding/json"

// accState is the serialisable union of all accumulator internal fields. Each
// accumulator maps only the fields it uses; compact JSON keys keep snapshots small.
type accState struct {
	Sum  float64     `json:"s,omitempty"`
	N    int64       `json:"n,omitempty"`
	Min  float64     `json:"mn,omitempty"`
	Max  float64     `json:"mx,omitempty"`
	Seen bool        `json:"e,omitempty"`
	Set  bool        `json:"t,omitempty"`
	Val  interface{} `json:"v,omitempty"`
}

func (a *sumAcc) snap() accState   { return accState{Sum: a.sum, Seen: a.seen} }
func (a *sumAcc) load(s accState)  { a.sum, a.seen = s.Sum, s.Seen }
func (a *avgAcc) snap() accState   { return accState{Sum: a.sum, N: a.n} }
func (a *avgAcc) load(s accState)  { a.sum, a.n = s.Sum, s.N }
func (a *minAcc) snap() accState   { return accState{Min: a.min, Seen: a.seen} }
func (a *minAcc) load(s accState)  { a.min, a.seen = s.Min, s.Seen }
func (a *maxAcc) snap() accState   { return accState{Max: a.max, Seen: a.seen} }
func (a *maxAcc) load(s accState)  { a.max, a.seen = s.Max, s.Seen }
func (a *countAcc) snap() accState { return accState{N: a.n} }
func (a *countAcc) load(s accState) { a.n = s.N }
func (a *firstAcc) snap() accState { return accState{Val: a.val, Set: a.set} }
func (a *firstAcc) load(s accState) { a.val, a.set = s.Val, s.Set }
func (a *lastAcc) snap() accState  { return accState{Val: a.val, Set: a.set} }
func (a *lastAcc) load(s accState) { a.val, a.set = s.Val, s.Set }

// --- operator snapshot ---

type opSnapshot struct {
	Version      int           `json:"version"`
	MaxEventTime int64         `json:"maxEventTime"`
	HasEvent     bool          `json:"hasEvent"`
	Watermark    int64         `json:"watermark"`
	Keys         []keySnapshot `json:"keys"`
}

type keySnapshot struct {
	KeyVals []interface{}    `json:"key"`
	Windows []windowSnapshot `json:"windows"`
}

type windowSnapshot struct {
	Start      int64      `json:"start"`
	End        int64      `json:"end"`
	Accs       []accState `json:"accs"`
	EarlyFired int64      `json:"earlyFired,omitempty"`
}

// Snapshot serialises the operator's keyed window state to JSON so a streaming
// pipeline can persist it and resume after a restart. The spec/aggregations are
// NOT included — Restore into an operator built with the same config.
func (o *Operator) Snapshot() ([]byte, error) {
	snap := opSnapshot{
		Version:      1,
		MaxEventTime: o.maxEventTime,
		HasEvent:     o.hasEvent,
		Watermark:    o.currentWatermark,
	}
	for _, key := range o.store.keys() {
		st, ok := o.store.get(key)
		if !ok {
			continue
		}
		ks := keySnapshot{KeyVals: st.keyVals}
		for w, accs := range st.windows {
			ws := windowSnapshot{Start: w.Start, End: w.End, Accs: make([]accState, len(accs))}
			for i, a := range accs {
				ws.Accs[i] = a.snap()
			}
			if st.earlyFired != nil {
				ws.EarlyFired = st.earlyFired[w]
			}
			ks.Windows = append(ks.Windows, ws)
		}
		snap.Keys = append(snap.Keys, ks)
	}
	return json.Marshal(snap)
}

// Restore rebuilds keyed window state from a Snapshot, replacing any existing
// state. The operator must have been constructed with the same aggregations.
func (o *Operator) Restore(data []byte) error {
	var snap opSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	o.maxEventTime = snap.MaxEventTime
	o.hasEvent = snap.HasEvent
	o.currentWatermark = snap.Watermark
	o.store.clear()
	for _, ks := range snap.Keys {
		st := &keyState{keyVals: ks.KeyVals, windows: make(map[Window][]Accumulator, len(ks.Windows))}
		for _, ws := range ks.Windows {
			accs := make([]Accumulator, len(o.aggs))
			for i, ag := range o.aggs {
				acc, _ := newAccumulator(ag.Fn)
				if i < len(ws.Accs) {
					acc.load(ws.Accs[i])
				}
				accs[i] = acc
			}
			w := Window{Start: ws.Start, End: ws.End}
			st.windows[w] = accs
			if ws.EarlyFired != 0 {
				if st.earlyFired == nil {
					st.earlyFired = make(map[Window]int64)
				}
				st.earlyFired[w] = ws.EarlyFired
			}
		}
		o.store.put(keyString(ks.KeyVals), st)
	}
	return nil
}
