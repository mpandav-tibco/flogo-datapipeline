package podlog

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestThroughputSoak generates a large synthetic CRI log tree and measures how
// fast the collector drains it, asserting sustained throughput well above the
// customer's 15M records/day requirement (~174 rec/s average, ~1k rec/s peak).
//
// It runs a realistic mix: multiple namespaces/pods, ~10% multiline stack
// traces, and validates that every logical record is accounted for (read count
// == expected) — the reconciliation guarantee at scale.
//
// Skipped in -short mode (it writes a few hundred MB of temp data).
func TestThroughputSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in -short mode")
	}

	const (
		namespaces      = 5
		podsPerNS       = 4
		linesPerFile    = 20000 // 5*4*20000 = 400k logical records
		multilineEveryN = 10    // ~10% of records are 4-line stack traces
	)

	root := t.TempDir()
	expectedRecords := 0
	for ns := 0; ns < namespaces; ns++ {
		for pod := 0; pod < podsPerNS; pod++ {
			dir := filepath.Join(root,
				fmt.Sprintf("team-%d_app-%d_uid%d%d", ns, pod, ns, pod), "bwappnode")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			f, err := os.Create(filepath.Join(dir, "0.log"))
			if err != nil {
				t.Fatal(err)
			}
			w := bufio.NewWriterSize(f, 1<<20)
			base := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
			for i := 0; i < linesPerFile; i++ {
				ts := base.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano)
				if i%multilineEveryN == 0 {
					// A 4-physical-line stack trace = ONE logical record.
					fmt.Fprintf(w, "%s stderr F 2026-07-14 10:00:00 ERROR [worker] boom in app-%d\n", ts, pod)
					fmt.Fprintf(w, "%s stderr F \tat com.tibco.bw.Flow.execute(Flow.java:%d)\n", ts, i)
					fmt.Fprintf(w, "%s stderr F \tat com.tibco.bw.Engine.run(Engine.java:88)\n", ts)
					fmt.Fprintf(w, "%s stderr F \tat java.base/java.lang.Thread.run(Thread.java:840)\n", ts)
				} else {
					fmt.Fprintf(w, "%s stdout F 2026-07-14 10:00:00 INFO  [main] processed request %d for app-%d\n", ts, i, pod)
				}
				expectedRecords++
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			f.Close()
		}
	}

	src, err := newPodlogSource(map[string]interface{}{
		"root":          root,
		"node":          "soak-node",
		"fromBeginning": true,
		"bounded":       true,
		"pollMs":        20,
		"multiline":     map[string]interface{}{"startPattern": `^\d{4}-\d{2}-\d{2}`, "flushIdleMs": 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	start := time.Now()
	read := 0
	for {
		batch, err := src.ReadBatch(5000)
		if batch != nil {
			read += batch.Len()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	if read != expectedRecords {
		t.Fatalf("reconciliation at scale: read %d records, expected %d", read, expectedRecords)
	}
	rps := float64(read) / elapsed.Seconds()
	perDay := rps * 86400
	t.Logf("soak: %d logical records in %v = %.0f rec/s (%.1fM/day projected)",
		read, elapsed.Round(time.Millisecond), rps, perDay/1e6)

	// Must comfortably exceed the customer's 15M/day (~174 rec/s) requirement.
	if rps < 5000 {
		t.Errorf("throughput %.0f rec/s is below the 5000 rec/s soak floor", rps)
	}
}
