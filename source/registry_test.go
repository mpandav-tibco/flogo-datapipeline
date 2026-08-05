package source

import "testing"

// dummyFactory is a no-op source factory used to exercise the registry.
func dummyFactory(map[string]interface{}) (Source, error) { return nil, nil }

func TestSourceRegistryNewAndFormats(t *testing.T) {
	const name = "dummy-src-unit"
	Register(name, dummyFactory)

	if _, err := New(name, map[string]interface{}{}); err != nil {
		t.Fatalf("New(%q) err = %v", name, err)
	}

	if _, err := New("unknown-format-xyz", nil); err == nil {
		t.Fatal("New(unknown) expected an error")
	}

	found := false
	for _, f := range Formats() {
		if f == name {
			found = true
		}
	}
	if !found {
		t.Errorf("Formats() = %v, missing %q", Formats(), name)
	}
}

func TestSourceRegisterDuplicatePanics(t *testing.T) {
	const name = "dummy-src-dup"
	Register(name, dummyFactory)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate Register")
		}
	}()
	Register(name, dummyFactory)
}
