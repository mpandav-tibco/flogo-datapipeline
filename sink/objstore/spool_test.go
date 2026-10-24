package objstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func waitRows(t *testing.T, root string, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if rows, _ := rowsUnder(t, root); rows == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	rows, objs := rowsUnder(t, root)
	t.Fatalf("store holds %d rows in %d objects, want %d", rows, objs, want)
}

// With a spool, a checkpoint flush succeeds while the object store is down
// (rows are sealed on the node) and everything is uploaded once it is back.
func TestSpoolFlushSucceedsWhileStoreDown(t *testing.T) {
	root, spool := t.TempDir(), t.TempDir()
	storeDown.Store(true)
	defer storeDown.Store(false)
	s, err := newObjstoreSink("testflaky", map[string]interface{}{
		"key": "{pod}/part-{uuid}.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600,
		"spoolDir": spool, "spoolCloseWaitSeconds": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		b := oneRow(i)
		b.Columns = append(b.Columns, b.Columns[0])
		b.Columns[1].Name = "pod"
		b.Rows[0] = append(b.Rows[0], "pod-"+string(rune('a'+i%2)))
		if err := s.WriteBatch(b); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(); err != nil {
			t.Fatalf("Flush with the store down must succeed with a spool: %v", err)
		}
	}
	if rows, _ := rowsUnder(t, root); rows != 0 {
		t.Fatalf("store already holds %d rows while down", rows)
	}
	storeDown.Store(false)
	waitRows(t, root, 4)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	left, _ := filepath.Glob(filepath.Join(spool, "obj-*"))
	if len(left) != 0 {
		t.Fatalf("spool not emptied after upload: %v", left)
	}
}

// Objects sealed by a previous process are uploaded on startup; unsealed
// leftovers (never checkpointed) are discarded.
func TestSpoolRecoversAfterRestart(t *testing.T) {
	root, spool := t.TempDir(), t.TempDir()
	storeDown.Store(true)
	s, err := newObjstoreSink("testflaky", map[string]interface{}{
		"key": "logs/part-{uuid}.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600, "spoolDir": spool,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_ = s.WriteBatch(oneRow(i))
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// "Crash": an object being written but never sealed, and a new process.
	_ = os.WriteFile(filepath.Join(spool, "obj-crashed"+partSuffix), []byte("{\"msg\":\"x\"}\n"), 0o644)
	abs, _ := filepath.Abs(spool)
	spoolsMu.Lock()
	delete(spools, abs) // forget the old uploader (its workers keep failing harmlessly)
	spoolsMu.Unlock()
	storeDown.Store(false)
	if _, err := spoolFor(spool, "testflaky", map[string]interface{}{"root": root}, 0); err != nil {
		t.Fatal(err)
	}
	waitRows(t, root, 3)
	if _, err := os.Stat(filepath.Join(spool, "obj-crashed"+partSuffix)); !os.IsNotExist(err) {
		t.Fatal("unsealed leftover not discarded")
	}
}

// Over its cap, the spool refuses the checkpoint (back-pressure).
func TestSpoolFullBlocksCheckpoint(t *testing.T) {
	root, spool := t.TempDir(), t.TempDir()
	storeDown.Store(true)
	defer storeDown.Store(false)
	s, err := newObjstoreSink("testflaky", map[string]interface{}{
		"key": "logs/part-{uuid}.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600,
		"spoolDir": spool, "spoolMaxBytes": 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	var ferr error
	for i := 0; i < 5 && ferr == nil; i++ {
		_ = s.WriteBatch(oneRow(i))
		ferr = s.Flush()
	}
	if ferr == nil || !strings.Contains(ferr.Error(), "full") {
		t.Fatalf("want a spool-full error, got %v", ferr)
	}
}

// An object another worker already uploaded (stale listing) or orphaned
// metadata is skipped silently: no failure count, no backoff, no retry loop.
func TestSpoolSkipsVanishedObjects(t *testing.T) {
	root, spool := t.TempDir(), t.TempDir()
	u, err := spoolFor(spool, "testflaky", map[string]interface{}{"root": root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(spool, "obj-orphan"+metaSuffix)
	if err := os.WriteFile(orphan, []byte(`{"key":"x"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	u.wake <- struct{}{}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(orphan); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("orphan metadata not dropped")
		}
		time.Sleep(20 * time.Millisecond)
	}
	u.mu.Lock()
	failures := u.failures
	u.mu.Unlock()
	if failures != 0 {
		t.Fatalf("vanished object counted as %d upload failures", failures)
	}
}

// spoolDir "none" disables the spool (resolvers often treat "" as unset):
// flushes upload synchronously and fail while the store is down.
func TestSpoolDirNoneDisablesSpool(t *testing.T) {
	root := t.TempDir()
	storeDown.Store(true)
	defer storeDown.Store(false)
	s, err := newObjstoreSink("testflaky", map[string]interface{}{
		"key": "logs/part-{uuid}.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600,
		"spoolDir": "None",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.WriteBatch(oneRow(1))
	if err := s.Flush(); err == nil {
		t.Fatal("Flush succeeded with the store down: spool was not disabled")
	}
	if _, err := os.Stat("None"); !os.IsNotExist(err) {
		t.Fatal(`a spool directory named "None" was created`)
	}
}
