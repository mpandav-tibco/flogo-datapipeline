package checkpoint

import (
	"encoding/json"
	"testing"
)

// dummyCheckpoint is a no-op Checkpoint used to exercise the registry.
type dummyCheckpoint struct{}

func (dummyCheckpoint) Save(string, *Token) error   { return nil }
func (dummyCheckpoint) Load(string) (*Token, error) { return nil, nil }
func (dummyCheckpoint) Delete(string) error         { return nil }
func (dummyCheckpoint) Close() error                { return nil }

func TestCheckpointRegistry(t *testing.T) {
	const name = "dummy-ckpt-unit"
	Register(name, func(map[string]interface{}) (Checkpoint, error) { return dummyCheckpoint{}, nil })

	c, err := New(name, nil)
	if err != nil {
		t.Fatalf("New(%q) err = %v", name, err)
	}
	if c == nil {
		t.Fatal("New returned nil checkpoint")
	}

	if _, err := New("unknown-ckpt-xyz", nil); err == nil {
		t.Fatal("New(unknown) expected an error")
	}
}

func TestCheckpointRegisterDuplicatePanics(t *testing.T) {
	const name = "dummy-ckpt-dup"
	Register(name, func(map[string]interface{}) (Checkpoint, error) { return dummyCheckpoint{}, nil })
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate Register")
		}
	}()
	Register(name, func(map[string]interface{}) (Checkpoint, error) { return dummyCheckpoint{}, nil })
}

// fakeCursor implements CursorProvider.
type fakeCursor struct{}

func (fakeCursor) Cursor() json.RawMessage { return json.RawMessage(`{"off":42}`) }

func TestNewToken(t *testing.T) {
	// Without a CursorProvider, Payload stays nil (linear-skip resume).
	tk := NewToken("csv", 100, nil)
	if tk.Format != "csv" || tk.RowsProcessed != 100 {
		t.Fatalf("NewToken basic fields = %+v", tk)
	}
	if tk.Payload != nil {
		t.Errorf("Payload = %s, want nil", tk.Payload)
	}

	// With a CursorProvider, Payload is populated for O(1) resume.
	tk2 := NewToken("csv", 200, fakeCursor{})
	if string(tk2.Payload) != `{"off":42}` {
		t.Errorf("Payload = %s, want the cursor blob", tk2.Payload)
	}

	// Token round-trips through JSON.
	b, err := json.Marshal(tk2)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Token
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.RowsProcessed != 200 || string(back.Payload) != `{"off":42}` {
		t.Errorf("round-trip token = %+v", back)
	}
}

func TestNoopCheckpoint(t *testing.T) {
	var c Checkpoint = NoopCheckpoint{}
	if err := c.Save("p", &Token{}); err != nil {
		t.Errorf("Save: %v", err)
	}
	tk, err := c.Load("p")
	if err != nil || tk != nil {
		t.Errorf("Load = %v, %v; want nil, nil", tk, err)
	}
	if err := c.Delete("p"); err != nil {
		t.Errorf("Delete: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
