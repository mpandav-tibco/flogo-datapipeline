// Package router registers a conditional router (a.k.a. "split") transform that
// N-way routes rows to named sinks by ordered predicates — generalizing the
// two-way DLQ branch into an arbitrary conditional split.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/router"
//
// Config:
//
//	{
//	  "type": "router",
//	  "routes": [
//	    { "name": "high", "when": "amount > 1000", "sinkType": "jsonlines", "sinkConfig": { "path": "./high.jsonl" } },
//	    { "name": "eu",   "when": "region = 'EU'",  "sinkType": "csv",       "sinkConfig": { "path": "./eu.csv" } }
//	  ],
//	  "defaultSinkType": "jsonlines",
//	  "defaultSinkConfig": { "path": "./other.jsonl" },
//	  "onUnmatched": "pass"
//	}
//
// Semantics:
//   - Each row is tested against the routes IN ORDER; it is written to the FIRST
//     route whose "when" predicate matches, then removed from the stream.
//   - A row matching no route goes to the default sink when configured; otherwise
//     the "onUnmatched" policy applies — "pass" (default) forwards it downstream to
//     the pipeline's main sink / next transform, "drop" discards it.
//   - The "when" grammar is exactly the filter transform's "expr".
//
// The router owns external sinks, so it always runs single-threaded (it
// implements transform.SinkOwner): the engine never parallelizes a pipeline
// containing it, which would otherwise open duplicate writers per worker.
//
// For a DAG-authored (graph.nodes/edges) pipeline, PREFER native conditional/
// otherwise graph edges wired directly to sink nodes over this transform:
// route "high" becomes an edge {"type":"expression","value":"amount > 1000"}
// to a "high" sink node, and the default becomes an "otherwise" edge (omit it
// entirely for onUnmatched:"drop"). Two real benefits over this transform's
// owned sinks: every route's row count is visible in graphStats / the Studio
// canvas (an owned sink is invisible to the graph's node/edge model), and the
// node is no longer forced single-threaded (moot in the graph runner today,
// which never parallelizes within one node anyway — but a real win if that
// changes). The one thing to get right by hand: the graph's conditional edges
// are TEE-style (a row matching multiple conditions gets copied to each),
// unlike this transform's exclusive first-match-wins ordering — write
// mutually-exclusive predicates (e.g. a later route explicitly excludes an
// earlier one's range) to replicate exclusive routing; see
// TestGraphRun_NativeEdgeRouting in action/pipeline/graph_run_test.go for a
// worked example mirroring this exact config. This transform is NOT
// deprecated — it remains the only option for the LINEAR (non-graph) model.
package router

import (
	"errors"
	"fmt"
	"strings"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	"github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
)

var rlog = corelog.ChildLogger(corelog.RootLogger(), "router")

func init() {
	transform.Register("router", newRouter)
	transform.Register("split", newRouter) // alias
}

type route struct {
	name  string
	when  filter.Predicate
	snk   sink.Sink
	count int64
}

type routerTransform struct {
	routes      []*route
	defaultSink sink.Sink
	onUnmatched string // "pass" | "drop" (consulted only when defaultSink == nil)
	passCount   int64
	dropCount   int64
	defCount    int64
}

func newRouter(cfg map[string]interface{}) (transform.Transform, error) {
	routesRaw, ok := cfg["routes"].([]interface{})
	if !ok || len(routesRaw) == 0 {
		return nil, fmt.Errorf("router transform: 'routes' must be a non-empty array")
	}

	rt := &routerTransform{}
	// On any construction error, close whatever sinks we already opened so a bad
	// config never leaks file handles / connections.
	cleanup := func() {
		for _, r := range rt.routes {
			if r.snk != nil {
				_ = r.snk.Close()
			}
		}
	}

	for i, item := range routesRaw {
		m, ok := item.(map[string]interface{})
		if !ok {
			cleanup()
			return nil, fmt.Errorf("router transform: route[%d] must be an object", i)
		}
		whenExpr, _ := m["when"].(string)
		if whenExpr == "" {
			cleanup()
			return nil, fmt.Errorf("router transform: route[%d]: 'when' is required", i)
		}
		pred, err := filter.Compile(whenExpr)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("router transform: route[%d]: parse when %q: %w", i, whenExpr, err)
		}
		sinkType, _ := m["sinkType"].(string)
		if sinkType == "" {
			cleanup()
			return nil, fmt.Errorf("router transform: route[%d]: 'sinkType' is required", i)
		}
		sinkCfg, _ := m["sinkConfig"].(map[string]interface{})
		if sinkCfg == nil {
			sinkCfg = map[string]interface{}{}
		}
		snk, err := sink.New(sinkType, sinkCfg)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("router transform: route[%d] sink: %w", i, err)
		}
		name, _ := m["name"].(string)
		if name == "" {
			name = fmt.Sprintf("route%d", i)
		}
		rt.routes = append(rt.routes, &route{name: name, when: pred, snk: snk})
	}

	// Optional default sink for rows matching no route.
	if dt, _ := cfg["defaultSinkType"].(string); dt != "" {
		dcfg, _ := cfg["defaultSinkConfig"].(map[string]interface{})
		if dcfg == nil {
			dcfg = map[string]interface{}{}
		}
		ds, err := sink.New(dt, dcfg)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("router transform: default sink: %w", err)
		}
		rt.defaultSink = ds
	}

	rt.onUnmatched, _ = cfg["onUnmatched"].(string)
	switch rt.onUnmatched {
	case "", "pass":
		rt.onUnmatched = "pass"
	case "drop":
		// keep as-is
	default:
		cleanup()
		return nil, fmt.Errorf("router transform: onUnmatched must be 'pass' or 'drop', got %q", rt.onUnmatched)
	}

	return rt, nil
}

func (r *routerTransform) Name() string { return "router" }

// OwnsExternalSink marks the router as sink-owning so the engine keeps it on the
// single-threaded path (see transform.SinkOwner).
func (r *routerTransform) OwnsExternalSink() bool { return true }

func (r *routerTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	// Per-route row buffers (indices parallel r.routes), plus default + passthrough.
	routeRows := make([][][]interface{}, len(r.routes))
	var defRows, passRows [][]interface{}

	var row map[string]interface{}
	for i := range in.Rows {
		row = in.RowMapInto(i, row)
		matched := -1
		for j, rt := range r.routes {
			if rt.when.Eval(row) {
				matched = j
				break
			}
		}
		switch {
		case matched >= 0:
			routeRows[matched] = append(routeRows[matched], in.Rows[i])
		case r.defaultSink != nil:
			defRows = append(defRows, in.Rows[i])
		case r.onUnmatched == "pass":
			passRows = append(passRows, in.Rows[i])
		default: // "drop"
			r.dropCount++
		}
	}

	// Write each non-empty route buffer to its sink.
	for j, rows := range routeRows {
		if len(rows) == 0 {
			continue
		}
		if err := r.writeRows(r.routes[j].snk, in.Columns, rows); err != nil {
			return nil, fmt.Errorf("router: route %q sink write: %w", r.routes[j].name, err)
		}
		r.routes[j].count += int64(len(rows))
	}
	if len(defRows) > 0 {
		if err := r.writeRows(r.defaultSink, in.Columns, defRows); err != nil {
			return nil, fmt.Errorf("router: default sink write: %w", err)
		}
		r.defCount += int64(len(defRows))
	}

	if len(passRows) == 0 {
		return nil, nil
	}
	r.passCount += int64(len(passRows))
	out := record.Get()
	out.Columns = in.Columns
	out.Rows = passRows
	return out, nil
}

// writeRows wraps rows in a transient batch and writes them to snk. The batch is
// returned to the pool after the write; the sink must not retain it (the Sink
// contract) — matching how the validate transform routes to its error sink.
func (r *routerTransform) writeRows(snk sink.Sink, cols []record.Column, rows [][]interface{}) error {
	b := record.Get()
	b.Columns = cols
	b.Rows = rows
	err := snk.WriteBatch(b)
	record.Put(b)
	return err
}

func (r *routerTransform) Close() error {
	var errs []error
	for _, rt := range r.routes {
		if rt.snk != nil {
			_ = rt.snk.Flush()
			if err := rt.snk.Close(); err != nil {
				errs = append(errs, fmt.Errorf("route %q: %w", rt.name, err))
			}
		}
	}
	if r.defaultSink != nil {
		_ = r.defaultSink.Flush()
		if err := r.defaultSink.Close(); err != nil {
			errs = append(errs, fmt.Errorf("default: %w", err))
		}
	}

	var sb strings.Builder
	for _, rt := range r.routes {
		fmt.Fprintf(&sb, "%s=%d ", rt.name, rt.count)
	}
	rlog.Infof("router closed — %sdefault=%d passed=%d dropped=%d",
		sb.String(), r.defCount, r.passCount, r.dropCount)
	return errors.Join(errs...)
}
