package pipeline

import (
	"encoding/json"
	"sync"
	"testing"
)

// TestFactoryReplaceConcurrent fires many concurrent replaces of the same ref.
// With Replace serialised, they must not panic, double-shut-down, or leave the
// registry inconsistent: the ref stays registered and there is exactly one
// entry. Run under -race it also guards the actions-map accesses.
func TestFactoryReplaceConcurrent(t *testing.T) {
	f := &Factory{}
	mkDef := func() *Definition {
		d := &Definition{}
		if err := json.Unmarshal([]byte(`{"source":{"type":"csv"},"sink":{"type":"null"}}`), d); err != nil {
			t.Fatalf("unmarshal def: %v", err)
		}
		return d
	}

	const ref = "res://pipeline:concurrent"
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.Replace(ref, mkDef()); err != nil {
				t.Errorf("Replace: %v", err)
			}
		}()
	}
	wg.Wait()

	actions := f.Actions()
	if actions[ref] == nil {
		t.Fatal("ref not registered after concurrent replaces")
	}
	if n := len(actions); n != 1 {
		t.Errorf("registered actions = %d, want 1", n)
	}
}
