package objstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/sink"
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
		"spoolDir": spool, "spoolMinFreePercent": 0, "spoolCloseWaitSeconds": 1,
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
		"key": "logs/part-{uuid}.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600, "spoolDir": spool, "spoolMinFreePercent": 0, "spoolCloseWaitSeconds": 1,
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
	_ = s.Close() // store still down: the sealed objects stay; the uploader stops
	storeDown.Store(false)
	u, err := spoolFor(spool, "testflaky", map[string]interface{}{"root": root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer u.release()
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
		"spoolDir": spool, "spoolMinFreePercent": 0, "spoolMaxBytes": 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { storeDown.Store(true); _ = s.Close() }()
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
	defer u.release()
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
	defer s.Close()
	_ = s.WriteBatch(oneRow(1))
	if err := s.Flush(); err == nil {
		t.Fatal("Flush succeeded with the store down: spool was not disabled")
	}
	if _, err := os.Stat("None"); !os.IsNotExist(err) {
		t.Fatal(`a spool directory named "None" was created`)
	}
}

// The last sink to close stops the uploader: no goroutine outlives its users
// (a restarted run gets a fresh uploader that recovers what is left on disk).
func TestSpoolStopsWithLastUser(t *testing.T) {
	root, spool := t.TempDir(), t.TempDir()
	cfg := map[string]interface{}{
		"key": "{pod}/part-{uuid}.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600,
		"spoolDir": spool, "spoolMinFreePercent": 0, "spoolCloseWaitSeconds": 1,
	}
	a, err := newObjstoreSink("testflaky", cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newObjstoreSink("testflaky", cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []sink.Sink{a, b} {
		x := oneRow(1)
		x.Columns = append(x.Columns, x.Columns[0])
		x.Columns[1].Name = "pod"
		x.Rows[0] = append(x.Rows[0], "p")
		if err := s.WriteBatch(x); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(); err != nil { // children closed after flush; parent keeps its reference
			t.Fatal(err)
		}
	}
	abs, _ := filepath.Abs(spool)
	running := func() bool { spoolsMu.Lock(); defer spoolsMu.Unlock(); _, ok := spools[abs]; return ok }
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if !running() {
		t.Fatal("uploader stopped while another sink still uses it")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if running() {
		t.Fatal("uploader still running after its last user closed")
	}
	waitRows(t, root, 2)
}

// Below spoolMinFreePercent free on the node filesystem, a spool with a
// backlog stops growing (back-pressure) instead of pushing the node into
// DiskPressure; without a backlog, flushing goes on.
func TestSpoolStopsGrowingOnLowDisk(t *testing.T) {
	if _, _, ok := diskFree(t.TempDir()); !ok {
		t.Skip("disk statistics unavailable on this OS")
	}
	root, spool := t.TempDir(), t.TempDir()
	storeDown.Store(true)
	defer storeDown.Store(false)
	s, err := newObjstoreSink("testflaky", map[string]interface{}{
		"key": "logs/part-{uuid}.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600,
		"spoolDir": spool, "spoolMinFreePercent": 100, "spoolCloseWaitSeconds": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_ = s.WriteBatch(oneRow(1))
	if err := s.Flush(); err != nil {
		t.Fatalf("first flush (no backlog) must succeed: %v", err)
	}
	_ = s.WriteBatch(oneRow(2))
	if err := s.Flush(); err == nil || !strings.Contains(err.Error(), "spoolMinFreePercent") {
		t.Fatalf("want a low-disk refusal with a backlog, got %v", err)
	}
	storeDown.Store(false)
	waitRows(t, root, 1)
	deadline := time.Now().Add(5 * time.Second)
	for s.Flush() != nil {
		if time.Now().After(deadline) {
			t.Fatal("flush still refused after the backlog was uploaded")
		}
		time.Sleep(50 * time.Millisecond)
	}
	waitRows(t, root, 2)
}

// Corrupt metadata cannot be fixed by retrying: the object is moved aside
// (data kept for manual recovery) and no longer blocks the spool.
func TestSpoolSetsAsideCorruptMetadata(t *testing.T) {
	root, spool := t.TempDir(), t.TempDir()
	base := filepath.Join(spool, "obj-bad")
	_ = os.WriteFile(base+dataSuffix, []byte("{\"msg\":\"x\"}\n"), 0o644)
	_ = os.WriteFile(base+metaSuffix, []byte(`{"key":`), 0o644)
	u, err := spoolFor(spool, "testflaky", map[string]interface{}{"root": root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer u.release()
	if !u.waitEmpty(5 * time.Second) {
		t.Fatal("corrupt object still counted as pending")
	}
	if m, _ := filepath.Glob(base + dataSuffix + ".corrupt-*"); len(m) != 1 {
		t.Fatalf("data not kept aside: %v", m)
	}
}
