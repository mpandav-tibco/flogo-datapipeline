package sink

import "testing"

// dummyFactory is a no-op sink factory used to exercise the registry.
func dummyFactory(map[string]interface{}) (Sink, error) { return nil, nil }

func TestSinkRegistryNewAndTypes(t *testing.T) {
	const name = "dummy-sink-unit"
	Register(name, dummyFactory)

	if _, err := New(name, map[string]interface{}{}); err != nil {
		t.Fatalf("New(%q) err = %v", name, err)
	}

	if _, err := New("unknown-sink-xyz", nil); err == nil {
		t.Fatal("New(unknown) expected an error")
	}

	found := false
	for _, ty := range Types() {
		if ty == name {
			found = true
		}
	}
	if !found {
		t.Errorf("Types() = %v, missing %q", Types(), name)
	}
}

func TestSinkRegisterDuplicatePanics(t *testing.T) {
	const name = "dummy-sink-dup"
	Register(name, dummyFactory)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate Register")
		}
	}()
	Register(name, dummyFactory)
}
