package fileutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	if err := WriteFileAtomic(path, []byte("v1"), 0o644); err != nil {
		t.Fatalf("write v1: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != "v1" {
		t.Errorf("read v1: got %q", got)
	}

	// Overwrite (atomic replace) with different-length content.
	if err := WriteFileAtomic(path, []byte("v2-longer"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != "v2-longer" {
		t.Errorf("read v2: got %q", got)
	}

	// No temp files must be left behind on success.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "state.json" {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("expected only state.json, got %v", names)
	}

	// Permissions are honored.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o644 {
		t.Errorf("perm = %v, want 0644", fi.Mode().Perm())
	}
}

func TestWriteFileAtomic_ErrorLeavesNoTemp(t *testing.T) {
	// A non-existent directory makes CreateTemp fail; no temp file should linger.
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist", "state.json")
	if err := WriteFileAtomic(missing, []byte("x"), 0o644); err == nil {
		t.Fatal("expected an error writing into a missing directory")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Errorf("expected no files, got %d", len(entries))
	}
}
