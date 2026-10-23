package windowing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// accState is the serialisable union of all accumulator internal fields. Each
// accumulator maps only the fields it uses; compact JSON keys keep snapshots small.
type accState struct {
	Sum  float64     `json:"s,omitempty"`
	N    int64       `json:"n,omitempty"`
	Min  float64     `json:"mn,omitempty"`
	Max  float64     `json:"mx,omitempty"`
	Seen bool        `json:"e,omitempty"`
	Set  bool        `json:"t,omitempty"`
	Val  *typedValue `json:"v,omitempty"`
	// Decimal-mode fields hold the exact value as a string so precision survives
	// the JSON round-trip (a JSON number would re-round through float64).
	IsDec bool   `json:"d,omitempty"`
	DSum  string `json:"ds,omitempty"`
	DMin  string `json:"dmn,omitempty"`
	DMax  string `json:"dmx,omitempty"`
}

func (a *sumAcc) snap() accState {
	if a.isDec {
		return accState{IsDec: true, DSum: a.dsum.String(), Seen: a.seen}
	}
	return accState{Sum: a.sum, Seen: a.seen}
}
func (a *sumAcc) load(s accState) {
	a.seen = s.Seen
	if s.IsDec {
		a.isDec = true
		a.dsum, _ = record.ParseDecimal(s.DSum)
	} else {
		a.sum = s.Sum
	}
}
func (a *avgAcc) snap() accState {
	if a.isDec {
		return accState{IsDec: true, DSum: a.dsum.String(), N: a.n}
	}
	return accState{Sum: a.sum, N: a.n}
}
func (a *avgAcc) load(s accState) {
	a.n = s.N
	if s.IsDec {
		a.isDec = true
		a.dsum, _ = record.ParseDecimal(s.DSum)
	} else {
		a.sum = s.Sum
	}
}
func (a *minAcc) snap() accState {
	if a.isDec {
		return accState{IsDec: true, DMin: a.dmin.String(), Seen: a.seen}
	}
	return accState{Min: a.min, Seen: a.seen}
}
func (a *minAcc) load(s accState) {
	a.seen = s.Seen
	if s.IsDec {
		a.isDec = true
		a.dmin, _ = record.ParseDecimal(s.DMin)
	} else {
		a.min = s.Min
	}
}
func (a *maxAcc) snap() accState {
	if a.isDec {
		return accState{IsDec: true, DMax: a.dmax.String(), Seen: a.seen}
	}
	return accState{Max: a.max, Seen: a.seen}
}
func (a *maxAcc) load(s accState) {
	a.seen = s.Seen
	if s.IsDec {
		a.isDec = true
		a.dmax, _ = record.ParseDecimal(s.DMax)
	} else {
		a.max = s.Max
	}
}
func (a *countAcc) snap() accState  { return accState{N: a.n} }
func (a *countAcc) load(s accState) { a.n = s.N }
func (a *firstAcc) snap() accState  { return accState{Val: wrapVal(a.val), Set: a.set} }
func (a *firstAcc) load(s accState) { a.val, a.set = unwrapVal(s.Val), s.Set }
func (a *lastAcc) snap() accState   { return accState{Val: wrapVal(a.val), Set: a.set} }
func (a *lastAcc) load(s accState)  { a.val, a.set = unwrapVal(s.Val), s.Set }

// --- operator snapshot ---

// A snapshot is a single JSON object
// {version,maxEventTime,hasEvent,watermark,keys:[keySnapshot...]} whose "keys"
// array is streamed one element at a time on both encode and decode, so a spilled
// operator is snapshotted/restored without materialising its entire state at once.

type keySnapshot struct {
	KeyVals []typedValue     `json:"key"`
	Windows []windowSnapshot `json:"windows"`
}

type windowSnapshot struct {
	Start      int64      `json:"start"`
	End        int64      `json:"end"`
	Accs       []accState `json:"accs"`
	EarlyFired int64      `json:"earlyFired,omitempty"`
}

// Snapshot serialises the operator's keyed window state as a single JSON object
// so a streaming pipeline can persist it (as an opaque token) and resume after a
// restart. The "keys" array is built by marshalling one key at a time, so a
// spilled operator is snapshotted without loading its entire state into memory at
// once. The spec/aggregations are NOT included — Restore into an operator built
// with the same config.
func (o *Operator) Snapshot() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(`{"version":1,"maxEventTime":`)
	buf.WriteString(strconv.FormatInt(o.maxEventTime, 10))
	buf.WriteString(`,"hasEvent":`)
	buf.WriteString(strconv.FormatBool(o.hasEvent))
	buf.WriteString(`,"watermark":`)
	buf.WriteString(strconv.FormatInt(o.currentWatermark, 10))
	buf.WriteString(`,"keys":[`)
	first := true
	for _, key := range o.store.keys() {
		st, ok := o.store.get(key)
		if !ok {
			continue
		}
		b, err := json.Marshal(keyStateToSnapshot(st))
		if err != nil {
			return nil, err
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		buf.Write(b)
	}
	buf.WriteString("]}")
	return buf.Bytes(), nil
}

// keyStateToSnapshot converts one live keyState into its serialisable form.
// Shared by Snapshot and the spillable state store.
func keyStateToSnapshot(st *keyState) keySnapshot {
	ks := keySnapshot{KeyVals: wrapVals(st.keyVals)}
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
	return ks
}

// keySnapshotToState rebuilds a keyState from its serialised form, constructing
// fresh accumulators of the operator's aggregate types. Shared by Restore and
// the spillable state store.
func keySnapshotToState(ks keySnapshot, aggs []AggSpec) *keyState {
	st := &keyState{keyVals: unwrapVals(ks.KeyVals), windows: make(map[Window][]Accumulator, len(ks.Windows))}
	for _, ws := range ks.Windows {
		accs := make([]Accumulator, len(aggs))
		for i, ag := range aggs {
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
	return st
}

// encodeKeyState / decodeKeyState serialise a single keyState for spill-to-disk,
// reusing the same compact accumulator format as the operator snapshot.
func encodeKeyState(st *keyState) ([]byte, error) {
	return json.Marshal(keyStateToSnapshot(st))
}

func decodeKeyState(data []byte, aggs []AggSpec) (*keyState, error) {
	var ks keySnapshot
	if err := json.Unmarshal(data, &ks); err != nil {
		return nil, err
	}
	return keySnapshotToState(ks, aggs), nil
}

// Restore rebuilds keyed window state from a Snapshot, replacing any existing
// state. It streams the "keys" array one element at a time via a JSON decoder,
// so restoring a large (spilled) snapshot does not materialise the whole state in
// memory. The operator must have been constructed with the same aggregations.
func (o *Operator) Restore(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return fmt.Errorf("windowing: snapshot is not a JSON object")
	}
	o.store.clear()
	o.openWindowCount = 0
	o.hasEvent = false
	for dec.More() {
		nameTok, err := dec.Token()
		if err != nil {
			return err
		}
		switch nameTok {
		case "maxEventTime":
			if err := dec.Decode(&o.maxEventTime); err != nil {
				return err
			}
		case "hasEvent":
			if err := dec.Decode(&o.hasEvent); err != nil {
				return err
			}
		case "watermark":
			if err := dec.Decode(&o.currentWatermark); err != nil {
				return err
			}
		case "keys":
			if _, err := dec.Token(); err != nil { // consume '['
				return err
			}
			for dec.More() {
				var ks keySnapshot
				if err := dec.Decode(&ks); err != nil {
					return err
				}
				st := keySnapshotToState(ks, o.aggs)
				o.store.put(keyString(st.keyVals), st)
				o.openWindowCount += len(ks.Windows)
			}
			if _, err := dec.Token(); err != nil { // consume ']'
				return err
			}
		default: // "version" or any unknown field
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return err
			}
		}
	}
	o.rebuildDeadlines()
	return nil
}
