// Package broadcast registers a fan-out ("tee") transform that writes every
// row to N additional named sinks in parallel with the pipeline's normal flow
// — the "read once, stream anywhere" pattern (capture the data once, deliver
// it to every destination that needs it) common to CDC/replication tools.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/broadcast"
//
// Unlike the router/split transform (which is a conditional, EXCLUSIVE fan-out
// — each row goes to exactly one route, then is removed from the stream),
// broadcast is unconditional and NON-exclusive: every row is duplicated to
// every configured sink, and the SAME batch continues downstream afterwards
// (to the next transform, or the pipeline's own main sink) unless
// "passthrough" is explicitly set to false.
//
// Config:
//
//	{
//	  "type": "broadcast",
//	  "sinks": [
//	    { "name": "s3-archive",   "sinkType": "s3",    "sinkConfig": { "bucket": "raw-archive", "key": "orders/{date}.jsonl", "format": "json" } },
//	    { "name": "kafka-notify", "sinkType": "kafka",  "sinkConfig": { "brokers": "localhost:9092", "topic": "orders-changed" } }
//	  ],
//	  "passthrough": true
//	}
//
// Semantics:
//   - Each configured sink receives every row in the batch, unchanged (same
//     columns, same values) — a full duplicate, not a filtered subset.
//   - "passthrough" (default true) controls whether the ORIGINAL batch
//     continues downstream after being broadcast. Set to false to make
//     broadcast a terminal stage (e.g. pair with a "null" main sink so the
//     pipeline's only real writes are the broadcast sinks).
//   - Sink writes happen in the order configured; if any sink write fails, the
//     batch fails (the run reports the error) — broadcast does not silently
//     drop a destination that's behind or unreachable, since the point of
//     fan-out is that every destination receives every row.
//
// Like router, broadcast owns external sinks, so it always runs
// single-threaded (transform.SinkOwner) — the parallel path would otherwise
// open duplicate writers per worker to the same destination.
//
// For a DAG-authored (graph.nodes/edges) pipeline, PREFER plain unconditional
// (default/success) edges wired directly to sink nodes over this transform:
// each broadcast sink becomes its own edge from the source node with no
// condition at all (every EdgeSuccess edge already tees every row — no
// mutually-exclusive predicate juggling needed, unlike router's case), giving
// the same visible-in-graphStats benefit. Not deprecated — remains the only
// option for the LINEAR (non-graph) model.
package broadcast

import (
	"errors"
	"fmt"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

var blog = corelog.ChildLogger(corelog.RootLogger(), "broadcast")

func init() {
	transform.Register("broadcast", newBroadcast)
	transform.Register("tee", newBroadcast) // alias
}

type namedSink struct {
	name  string
	snk   sink.Sink
	count int64
}

type broadcastTransform struct {
	sinks       []*namedSink
	passthrough bool
}

func newBroadcast(cfg map[string]interface{}) (transform.Transform, error) {
	sinksRaw, ok := cfg["sinks"].([]interface{})
	if !ok || len(sinksRaw) == 0 {
		return nil, fmt.Errorf("broadcast transform: 'sinks' must be a non-empty array")
	}

	bt := &broadcastTransform{passthrough: true}
	if p, ok := cfg["passthrough"].(bool); ok {
		bt.passthrough = p
	}

	// On any construction error, close whatever sinks we already opened so a
	// bad config never leaks file handles / connections.
	cleanup := func() {
		for _, s := range bt.sinks {
			_ = s.snk.Close()
		}
	}

	for i, item := range sinksRaw {
		m, ok := item.(map[string]interface{})
		if !ok {
			cleanup()
			return nil, fmt.Errorf("broadcast transform: sinks[%d] must be an object", i)
		}
		sinkType, _ := m["sinkType"].(string)
		if sinkType == "" {
			cleanup()
			return nil, fmt.Errorf("broadcast transform: sinks[%d]: 'sinkType' is required", i)
		}
		sinkCfg, _ := m["sinkConfig"].(map[string]interface{})
		if sinkCfg == nil {
			sinkCfg = map[string]interface{}{}
		}
		snk, err := sink.New(sinkType, sinkCfg)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("broadcast transform: sinks[%d] sink: %w", i, err)
		}
		name, _ := m["name"].(string)
		if name == "" {
			name = fmt.Sprintf("sink%d", i)
		}
		bt.sinks = append(bt.sinks, &namedSink{name: name, snk: snk})
	}

	return bt, nil
}

func (b *broadcastTransform) Name() string { return "broadcast" }

// OwnsExternalSink marks broadcast as sink-owning so the engine keeps it on the
// single-threaded path (see transform.SinkOwner) — otherwise the parallel
// path's per-worker pipelines would each open their own writer to the same
// destination.
func (b *broadcastTransform) OwnsExternalSink() bool { return true }

func (b *broadcastTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	if in == nil || in.Len() == 0 {
		return in, nil
	}
	for _, ns := range b.sinks {
		if err := ns.snk.WriteBatch(in); err != nil {
			return nil, fmt.Errorf("broadcast: sink %q write: %w", ns.name, err)
		}
		ns.count += int64(in.Len())
	}
	if !b.passthrough {
		return nil, nil
	}
	return in, nil
}

func (b *broadcastTransform) Close() error {
	var errs []error
	for _, ns := range b.sinks {
		_ = ns.snk.Flush()
		if err := ns.snk.Close(); err != nil {
			errs = append(errs, fmt.Errorf("sink %q: %w", ns.name, err))
		}
	}
	summary := ""
	for _, ns := range b.sinks {
		summary += fmt.Sprintf("%s=%d ", ns.name, ns.count)
	}
	blog.Infof("broadcast closed — %s", summary)
	return errors.Join(errs...)
}
