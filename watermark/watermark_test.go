package watermark

import "testing"

// dummyStore is a no-op Store used to exercise the registry.
type dummyStore struct{}

func (dummyStore) Load(string) (*Mark, error) { return nil, nil }
func (dummyStore) Save(string, *Mark) error   { return nil }
func (dummyStore) Close() error               { return nil }

func TestWatermarkRegistry(t *testing.T) {
	const name = "dummy-wm-unit"
	Register(name, func(map[string]interface{}) (Store, error) { return dummyStore{}, nil })

	s, err := New(name, nil)
	if err != nil {
		t.Fatalf("New(%q) err = %v", name, err)
	}
	if s == nil {
		t.Fatal("New returned nil store")
	}

	if _, err := New("unknown-wm-xyz", nil); err == nil {
		t.Fatal("New(unknown) expected an error")
	}
}

func TestWatermarkRegisterDuplicatePanics(t *testing.T) {
	const name = "dummy-wm-dup"
	Register(name, func(map[string]interface{}) (Store, error) { return dummyStore{}, nil })
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate Register")
		}
	}()
	Register(name, func(map[string]interface{}) (Store, error) { return dummyStore{}, nil })
}

func TestNoopStore(t *testing.T) {
	var s Store = NoopStore{}
	m, err := s.Load("p")
	if err != nil || m != nil {
		t.Errorf("Load = %v, %v; want nil, nil", m, err)
	}
	if err := s.Save("p", &Mark{Column: "updated_at", Value: "1", ValueType: "int"}); err != nil {
		t.Errorf("Save: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
