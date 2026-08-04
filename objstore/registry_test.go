package objstore

import "testing"

func dummyFactory(map[string]interface{}) (Store, error) { return nil, nil }

func TestObjstoreRegistry(t *testing.T) {
	const name = "dummy-objstore-unit"
	Register(name, dummyFactory)

	if _, err := New(name, map[string]interface{}{}); err != nil {
		t.Fatalf("New(%q) err = %v", name, err)
	}
	if _, err := New("unknown-provider-xyz", nil); err == nil {
		t.Fatal("New(unknown) expected an error")
	}

	found := false
	for _, p := range Providers() {
		if p == name {
			found = true
		}
	}
	if !found {
		t.Errorf("Providers() = %v, missing %q", Providers(), name)
	}
}

func TestObjstoreRegisterDuplicatePanics(t *testing.T) {
	const name = "dummy-objstore-dup"
	Register(name, dummyFactory)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate Register")
		}
	}()
	Register(name, dummyFactory)
}
