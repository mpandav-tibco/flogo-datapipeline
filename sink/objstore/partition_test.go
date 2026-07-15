package objstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines" // register inner format
)

// fileStore is a test objstore backend that writes objects to a local directory,
// so the partitioned sink's per-record fan-out can be asserted end-to-end.
type fileStore struct{ root string }

func (s *fileStore) List(context.Context, string) ([]objstore.Object, error) { return nil, nil }
func (s *fileStore) GetReader(context.Context, string) (io.ReadCloser, error) {
	return nil, os.ErrNotExist
}
func (s *fileStore) PutWriter(_ context.Context, key string) (io.WriteCloser, error) {
	full := filepath.Join(s.root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return nil, err
	}
	return os.Create(full)
}
func (s *fileStore) Delete(context.Context, string) error { return nil }
func (s *fileStore) Close() error                         { return nil }

func init() {
	objstore.Register("testfile", func(cfg map[string]interface{}) (objstore.Store, error) {
		root, _ := cfg["root"].(string)
		return &fileStore{root: root}, nil
	})
}

func TestKeyFieldPlaceholders(t *testing.T) {
	cases := map[string][]string{
		"logs/{YYYY}/{MM}/part.ndjson":            nil,
		"{namespace}/{YYYY}/part.ndjson":          {"namespace"},
		"{namespace}/{node}/{DD}/x.ndjson":        {"namespace", "node"},
		"${date}/part-${uuid}.parquet":            nil,
		"{namespace}/{namespace}.ndjson":          {"namespace"}, // deduped
	}
	for tmpl, want := range cases {
		got := keyFieldPlaceholders(tmpl)
		if len(got) != len(want) {
			t.Errorf("%q: got %v want %v", tmpl, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%q: got %v want %v", tmpl, got, want)
			}
		}
	}
}

// TestPartitionedFanOut writes a batch spanning two namespaces through the
// partitioned sink and asserts each namespace lands in its own object with only
// its own rows — the tenancy requirement.
func TestPartitionedFanOut(t *testing.T) {
	root := t.TempDir()
	s, err := newObjstoreSink("testfile", map[string]interface{}{
		"key":    "{namespace}/{node}/part.ndjson",
		"format": "jsonlines",
		"root":   root,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*partitionedObjstoreSink); !ok {
		t.Fatalf("expected a partitioned sink, got %T", s)
	}

	batch := &record.RecordBatch{
		Columns: []record.Column{
			{Name: "namespace", Type: record.TypeString},
			{Name: "node", Type: record.TypeString},
			{Name: "msg", Type: record.TypeString},
		},
		Rows: [][]interface{}{
			{"team-a", "node-1", "a1"},
			{"team-b", "node-1", "b1"},
			{"team-a", "node-1", "a2"},
		},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Two objects: team-a/node-1/part.ndjson (2 rows) and team-b/... (1 row).
	var got []string
	_ = filepath.Walk(root, func(p string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() {
			rel, _ := filepath.Rel(root, p)
			got = append(got, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(got)
	if len(got) != 2 || got[0] != "team-a/node-1/part.ndjson" || got[1] != "team-b/node-1/part.ndjson" {
		t.Fatalf("expected two namespace-partitioned objects, got %v", got)
	}

	aData, _ := os.ReadFile(filepath.Join(root, "team-a", "node-1", "part.ndjson"))
	if c := countLines(string(aData)); c != 2 {
		t.Errorf("team-a object should have 2 rows, got %d: %s", c, aData)
	}
	bData, _ := os.ReadFile(filepath.Join(root, "team-b", "node-1", "part.ndjson"))
	if c := countLines(string(bData)); c != 1 {
		t.Errorf("team-b object should have 1 row, got %d: %s", c, bData)
	}
}

// TestSingleObjectPathUnchanged confirms a template without field placeholders
// still produces one object (backward compatible).
func TestSingleObjectPathUnchanged(t *testing.T) {
	root := t.TempDir()
	s, err := newObjstoreSink("testfile", map[string]interface{}{
		"key":    "logs/{YYYY}/all.ndjson",
		"format": "jsonlines",
		"root":   root,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*partitionedObjstoreSink); ok {
		t.Fatal("template without field placeholders must use the single-object path")
	}
	_ = s.Close()
}

// TestRollBySize writes enough rows to force multiple object rolls and asserts
// each rolled object has a manifest sidecar, and the manifests account for
// every row (the audit guarantee).
func TestRollBySize(t *testing.T) {
	root := t.TempDir()
	s, err := newObjstoreSink("testfile", map[string]interface{}{
		"key":          "logs/part.ndjson", // no {seq} → inserted automatically
		"format":       "jsonlines",
		"root":         root,
		"rollMaxBytes": 200, // small, forces several rolls
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*rollingObjstoreSink); !ok {
		t.Fatalf("expected a rolling sink, got %T", s)
	}

	total := 20
	for i := 0; i < total; i++ {
		batch := &record.RecordBatch{
			Columns: []record.Column{{Name: "msg", Type: record.TypeString}},
			Rows:    [][]interface{}{{fmt.Sprintf("log line number %02d with some padding text", i)}},
		}
		if err := s.WriteBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	var objects, manifests []string
	var recordSum int64
	_ = filepath.Walk(root, func(p string, info os.FileInfo, _ error) error {
		if info == nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if strings.HasSuffix(rel, ".manifest.json") {
			manifests = append(manifests, rel)
			data, _ := os.ReadFile(p)
			var m map[string]interface{}
			if err := json.Unmarshal(data, &m); err != nil {
				t.Errorf("manifest %s not valid JSON: %v", rel, err)
			}
			if rc, ok := m["record_count"].(float64); ok {
				recordSum += int64(rc)
			}
		} else {
			objects = append(objects, rel)
		}
		return nil
	})

	if len(objects) < 2 {
		t.Fatalf("expected multiple rolled objects, got %d: %v", len(objects), objects)
	}
	if len(manifests) != len(objects) {
		t.Errorf("expected %d manifests (one per object), got %d", len(objects), len(manifests))
	}
	if recordSum != int64(total) {
		t.Errorf("manifest record_count sum = %d, want %d (no row unaccounted)", recordSum, total)
	}
}

func countLines(s string) int {
	n := 0
	for _, r := range s {
		if r == '\n' {
			n++
		}
	}
	return n
}

var _ sink.Sink = (*partitionedObjstoreSink)(nil)
