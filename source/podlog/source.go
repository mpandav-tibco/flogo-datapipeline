// Package podlog registers a Kubernetes pod-log collector source that tails
// containerd/CRI-O container logs from node-local disk (/var/log/pods/...),
// reassembles multiline (BW6 / Java) stack traces into single records, enriches
// each record with namespace/pod/container context, and emits them into the
// pipeline. It is designed to replace a per-namespace Fluentd DaemonSet with a
// per-node collector that can prove no-loss via read == written + dlq
// reconciliation (see examples/20-podlog/IMPLEMENTATION-PLAN.md).
//
// Import with a blank import to activate:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/source/podlog"
//
// # Configuration keys (source.options in the .datapipeline resource)
//
//	root               string    CRI pod-log root (default "/var/log/pods")
//	node               string    Node name stamped onto records (optional)
//	positionFile       string    Crash-safe byte-offset registry path (optional)
//	fromBeginning      bool       Read existing files from offset 0 (default false = tail from EOF)
//	pollMs             int        Idle poll interval in ms (default 500)
//	bounded            bool       Test/drain mode: ReadBatch returns io.EOF once the
//	                              watched tree is fully drained and idle (default false — unbounded stream)
//	includeNamespaces  []string  If non-empty, only these namespaces are tailed
//	excludeNamespaces  []string  Namespaces to skip (e.g. kube-system)
//	multiline          object    { startPattern, flushIdleMs, maxLines }
//	  startPattern     string    Regexp a new logical record starts with (e.g. a timestamp).
//	                              When set, continuation lines are stitched into the prior record.
//	  flushIdleMs      int        Emit a pending stitched record after a file is idle this long (default 2000)
//	  maxLines         int        Safety cap on stitched lines per record (default 500; 0 = unlimited)
package podlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"encoding/json"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

var plog = corelog.ChildLogger(corelog.RootLogger(), "podlog-source")

func init() {
	source.Register("podlog", newPodlogSource)
}

// podlogSource implements source.Source (streaming) and checkpoint.CursorProvider.
//
// The tailer goroutine discovers and tails every CRI log file under root and
// feeds stitched, enriched records onto a buffered channel. ReadBatch drains
// that channel with a timed poll. The position registry advances only when
// Cursor() is called — which the pipeline does after the sink has flushed at a
// checkpoint interval — giving at-least-once delivery (crash replays from the
// last persisted offset, producing safe duplicates, never silent loss).
type podlogSource struct {
	cfg       podlogConfig
	positions *positionStore
	tailer    *tailer
	ctx       context.Context
	cancel    context.CancelFunc

	schema []record.Column

	// pending holds the highest delivered offset per file that ReadBatch has
	// handed to the pipeline but Cursor() has not yet persisted.
	mu      sync.Mutex
	pending map[string]position

	// idle tracks consecutive empty poll windows so bounded mode can decide the
	// watched tree is fully drained.
	idlePolls int
	closed    atomic.Bool
}

type podlogConfig struct {
	root          string
	node          string
	positionFile  string
	fromBeginning bool
	pollMs        int
	bounded       bool
	includeNS     []string
	excludeNS     []string
	startPattern  string
	flushIdleMs   int
	maxLines      int
}

// podlogColumns is the fixed schema every podlog record carries. Downstream a
// map transform typically adds ingest_time and node.
var podlogColumns = []record.Column{
	{Name: "namespace", Type: record.TypeString},
	{Name: "pod", Type: record.TypeString},
	{Name: "container", Type: record.TypeString},
	{Name: "uid", Type: record.TypeString},
	{Name: "stream", Type: record.TypeString},
	{Name: "log_time", Type: record.TypeString},
	{Name: "message", Type: record.TypeString},
	{Name: "node", Type: record.TypeString},
	{Name: "idempotency_key", Type: record.TypeString},
}

func newPodlogSource(cfg map[string]interface{}) (source.Source, error) {
	c := parseConfig(cfg)
	if c.root == "" {
		c.root = "/var/log/pods"
	}
	if c.pollMs <= 0 {
		c.pollMs = 500
	}
	if c.flushIdleMs <= 0 {
		c.flushIdleMs = 2000
	}

	var startMatcher matcher
	if c.startPattern != "" {
		re, err := regexp.Compile(c.startPattern)
		if err != nil {
			return nil, fmt.Errorf("podlog source: invalid multiline.startPattern %q: %w", c.startPattern, err)
		}
		startMatcher = re
	}

	ps, err := newPositionStore(c.positionFile)
	if err != nil {
		return nil, fmt.Errorf("podlog source: position store: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &tailer{
		root:          c.root,
		node:          c.node,
		fromBeginning: c.fromBeginning,
		pollMs:        c.pollMs,
		startPattern:  startMatcher,
		includeNS:     toSet(c.includeNS),
		excludeNS:     toSet(c.excludeNS),
		positions:     ps,
		flushIdle:     time.Duration(c.flushIdleMs) * time.Millisecond,
		bounded:       c.bounded,
		out:           make(chan enrichedRecord, 1024),
	}

	s := &podlogSource{
		cfg:       c,
		positions: ps,
		tailer:    t,
		ctx:       ctx,
		cancel:    cancel,
		schema:    podlogColumns,
		pending:   map[string]position{},
	}

	go t.run(ctx)
	plog.Infof("podlog source started — root=%s node=%s bounded=%t multiline=%t",
		c.root, c.node, c.bounded, startMatcher != nil)
	return s, nil
}

func (s *podlogSource) Schema() []record.Column { return s.schema }

// Skip is a no-op: the position registry (not a row count) owns resume for a
// tailing, unbounded, multi-file collector.
func (s *podlogSource) Skip(n int64) error { return nil }

// ReadBatch drains up to size stitched records from the tailer, polling for at
// most pollMs. In streaming mode an empty (non-nil) batch is returned when the
// poll window closes with no records. In bounded (test/drain) mode io.EOF is
// returned once the tree has produced records and then gone idle.
func (s *podlogSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed.Load() {
		return nil, io.EOF
	}

	deadline := time.Now().Add(time.Duration(s.cfg.pollMs) * time.Millisecond)
	batch := record.Get()
	if cap(batch.Rows) >= size {
		batch.Rows = batch.Rows[:0]
	} else {
		batch.Rows = make([][]interface{}, 0, size)
	}
	batch.Columns = s.schema

	got := false
	for len(batch.Rows) < size {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		timer := time.NewTimer(remaining)
		select {
		case er, ok := <-s.tailer.out:
			timer.Stop()
			if !ok {
				// Tailer closed its channel (context cancelled / drained).
				if len(batch.Rows) == 0 {
					record.Put(batch)
					return nil, io.EOF
				}
				return batch, nil
			}
			batch.Rows = append(batch.Rows, s.rowFor(er))
			s.trackPending(er)
			got = true
		case <-timer.C:
			// poll window elapsed
			goto done
		}
	}

done:
	if got {
		s.idlePolls = 0
		return batch, nil
	}
	// No records this poll window. In bounded mode EOF is signalled by the
	// tailer closing its channel (handled above); here we just return an empty
	// batch to keep the streaming pipeline loop alive.
	s.idlePolls++
	return batch, nil
}

// rowFor builds a schema-ordered row for one enriched record, stamping the node
// and a stable idempotency key so downstream consumers can dedupe replays.
func (s *podlogSource) rowFor(er enrichedRecord) []interface{} {
	key := idempotencyKey(s.cfg.node, er.fileKey, er.fp, er.endOffset)
	return []interface{}{
		er.meta.namespace,
		er.meta.pod,
		er.meta.container,
		er.meta.uid,
		er.stream,
		er.timestamp,
		er.message,
		s.cfg.node,
		key,
	}
}

// trackPending records the highest delivered offset per file so Cursor() can
// promote it into the durable registry after the sink acks.
func (s *podlogSource) trackPending(er enrichedRecord) {
	s.mu.Lock()
	cur, ok := s.pending[er.fileKey]
	if !ok || er.endOffset > cur.Offset {
		s.pending[er.fileKey] = position{Offset: er.endOffset, Fingerprint: er.fp}
	}
	s.mu.Unlock()
}

// Cursor implements checkpoint.CursorProvider. The pipeline calls it after the
// sink has flushed at a checkpoint interval, so every record handed out so far
// is durably written. That is exactly when it is safe to advance the position
// registry — implementing at-least-once via post-ack offset advance (Option B
// in the implementation plan). Returns the registry snapshot for audit.
func (s *podlogSource) Cursor() json.RawMessage {
	s.mu.Lock()
	for key, p := range s.pending {
		s.positions.set(key, p)
	}
	s.pending = map[string]position{}
	s.mu.Unlock()
	if err := s.positions.persist(); err != nil {
		plog.Warnf("podlog source: persist positions: %v", err)
	}
	return s.positions.snapshot()
}

func (s *podlogSource) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	s.cancel()
	// Best-effort final persist so the last acknowledged offsets survive a clean
	// shutdown (a crash falls back to the last checkpoint-time persist).
	s.mu.Lock()
	for key, p := range s.pending {
		s.positions.set(key, p)
	}
	s.pending = map[string]position{}
	s.mu.Unlock()
	return s.positions.persist()
}

// idempotencyKey is a stable per-record identifier:
// sha256(node + file-path + fingerprint + end-offset).
func idempotencyKey(node, fileKey, fp string, endOffset int64) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%d", node, fileKey, fp, endOffset)
	return hex.EncodeToString(h.Sum(nil))
}

// --- config parsing helpers ---

func parseConfig(cfg map[string]interface{}) podlogConfig {
	c := podlogConfig{
		root:          strFromCfg(cfg, "root"),
		node:          strFromCfg(cfg, "node"),
		positionFile:  strFromCfg(cfg, "positionFile"),
		fromBeginning: boolFromCfg(cfg, "fromBeginning"),
		pollMs:        intFromCfg(cfg, "pollMs"),
		bounded:       boolFromCfg(cfg, "bounded"),
		includeNS:     strSliceFromCfg(cfg, "includeNamespaces"),
		excludeNS:     strSliceFromCfg(cfg, "excludeNamespaces"),
	}
	if ml, ok := cfg["multiline"].(map[string]interface{}); ok {
		c.startPattern = strFromCfg(ml, "startPattern")
		c.flushIdleMs = intFromCfg(ml, "flushIdleMs")
		c.maxLines = intFromCfg(ml, "maxLines")
	}
	return c
}

func strFromCfg(m map[string]interface{}, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func boolFromCfg(m map[string]interface{}, k string) bool {
	if v, ok := m[k].(bool); ok {
		return v
	}
	return false
}

func intFromCfg(m map[string]interface{}, k string) int {
	switch v := m[k].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

func strSliceFromCfg(m map[string]interface{}, k string) []string {
	raw, ok := m[k].([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func toSet(vals []string) map[string]bool {
	if len(vals) == 0 {
		return nil
	}
	m := make(map[string]bool, len(vals))
	for _, v := range vals {
		m[v] = true
	}
	return m
}
