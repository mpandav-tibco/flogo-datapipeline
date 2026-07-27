// Package lineage emits OpenLineage run events for pipeline runs, so an operator
// can answer "where did this data come from" in a lineage backend (Marquez, or
// any OpenLineage-compatible collector).
//
// It produces standard OpenLineage RunEvent JSON (START / COMPLETE / FAIL /
// ABORT) naming the source and sink datasets plus schema and output-statistics
// facets, using only the standard library — no OpenLineage client dependency.
//
// Lineage is OFF by default. Enable it per pipeline via the observability config:
//
//	"observability": { "lineage": { "enabled": true, "transport": "http", "url": "http://marquez:5000/api/v1/lineage" } }
//
// or via the standard OpenLineage environment variables:
//
//	DP_LINEAGE=on|1|true         enable lineage emission
//	OPENLINEAGE_URL=<endpoint>   enable + POST events to this collector
//	OPENLINEAGE_NAMESPACE=<ns>   default job/dataset namespace
//
// Emission is best-effort: a transport error is logged and swallowed, never
// blocking or failing the pipeline.
package lineage

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	corelog "github.com/project-flogo/core/support/log"
)

const (
	producer         = "https://github.com/mpandav-tibco/flogo-datapipeline"
	runEventSchema   = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
	schemaFacetURL   = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/SchemaDatasetFacet"
	outStatsFacetURL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/OutputStatisticsOutputDatasetFacet"
	defaultNamespace = "flogo-datapipeline"
)

var llog = corelog.ChildLogger(corelog.RootLogger(), "lineage")

// Run-event lifecycle types.
const (
	Start    = "START"
	Complete = "COMPLETE"
	Fail     = "FAIL"
	Abort    = "ABORT"
)

// Field is one dataset schema column.
type Field struct {
	Name string
	Type string
}

// Dataset identifies an input or output dataset with an optional schema.
type Dataset struct {
	Namespace string
	Name      string
	Fields    []Field
}

// Event is a transport-agnostic lineage event the engine emits at run-lifecycle
// transitions.
type Event struct {
	Type     string // Start | Complete | Fail | Abort
	RunID    string // pipeline run id (mapped to a stable UUID)
	Job      string // job name (pipeline ref)
	Inputs   []Dataset
	Outputs  []Dataset
	RowCount int64 // output row count for Complete; negative = omit
}

// Emitter sends OpenLineage run events. Emission is best-effort and must never
// block or fail the pipeline.
type Emitter interface {
	Emit(ev Event)
	Close() error
}

// Config controls emitter construction (from the pipeline observability block).
type Config struct {
	Enabled   bool   `json:"enabled,omitempty"`
	Transport string `json:"transport,omitempty"` // console | file | http (default console)
	URL       string `json:"url,omitempty"`       // http endpoint
	File      string `json:"file,omitempty"`      // file transport path
	Namespace string `json:"namespace,omitempty"` // default job/dataset namespace
	Job       string `json:"job,omitempty"`       // job-name override
}

// Noop discards all events — the zero-cost default when lineage is disabled.
type Noop struct{}

// Emit discards the event.
func (Noop) Emit(Event) {}

// Close is a no-op.
func (Noop) Close() error { return nil }

// New builds an Emitter from cfg, with environment overrides:
//
//	DP_LINEAGE = on|1|true       → enable
//	OPENLINEAGE_URL = <url>       → enable + use the http transport to that URL
//	OPENLINEAGE_NAMESPACE = <ns>  → default namespace
//
// It returns Noop{} when disabled. Misconfiguration (missing url/file for the
// selected transport) is returned as an error so it surfaces at startup.
func New(cfg Config) (Emitter, error) {
	enabled := cfg.Enabled
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DP_LINEAGE"))) {
	case "on", "1", "true", "yes":
		enabled = true
	}
	transport := strings.ToLower(strings.TrimSpace(cfg.Transport))
	url := cfg.URL
	if u := strings.TrimSpace(os.Getenv("OPENLINEAGE_URL")); u != "" {
		url = u
		if transport == "" {
			transport = "http"
		}
		enabled = true
	}
	if !enabled {
		return Noop{}, nil
	}

	ns := cfg.Namespace
	if ns == "" {
		ns = strings.TrimSpace(os.Getenv("OPENLINEAGE_NAMESPACE"))
	}
	if ns == "" {
		ns = defaultNamespace
	}

	e := &emitter{namespace: ns, job: cfg.Job}
	switch transport {
	case "", "console":
		e.sink = &writerSink{w: os.Stdout}
	case "file":
		if cfg.File == "" {
			return nil, fmt.Errorf("lineage: transport 'file' requires a 'file' path")
		}
		f, err := os.OpenFile(cfg.File, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		e.sink = &writerSink{w: f, closer: f}
	case "http":
		if url == "" {
			return nil, fmt.Errorf("lineage: transport 'http' requires a 'url' (or OPENLINEAGE_URL)")
		}
		e.sink = &httpSink{url: url, client: &http.Client{Timeout: 5 * time.Second}}
	default:
		return nil, fmt.Errorf("lineage: unknown transport %q (want console|file|http)", transport)
	}
	return e, nil
}

type emitter struct {
	namespace string
	job       string
	sink      lineageSink
}

func (e *emitter) Emit(ev Event) {
	b, err := json.Marshal(e.build(ev))
	if err != nil {
		return
	}
	if err := e.sink.send(b); err != nil {
		llog.Warnf("lineage: emit %s failed: %v", ev.Type, err)
	}
}

func (e *emitter) Close() error { return e.sink.Close() }

// build converts a transport-agnostic Event into an OpenLineage RunEvent.
func (e *emitter) build(ev Event) olEvent {
	job := ev.Job
	if e.job != "" {
		job = e.job
	}
	return olEvent{
		EventType: ev.Type,
		EventTime: time.Now().UTC().Format(time.RFC3339Nano),
		Producer:  producer,
		SchemaURL: runEventSchema,
		Run:       olRun{RunID: uuidFromString(ev.RunID)},
		Job:       olJob{Namespace: e.namespace, Name: job},
		Inputs:    e.datasets(ev.Inputs, false, -1),
		Outputs:   e.datasets(ev.Outputs, ev.Type == Complete, ev.RowCount),
	}
}

func (e *emitter) datasets(ds []Dataset, withStats bool, rowCount int64) []olDataset {
	if len(ds) == 0 {
		return nil
	}
	out := make([]olDataset, 0, len(ds))
	for _, d := range ds {
		ns := d.Namespace
		if ns == "" {
			ns = e.namespace
		}
		od := olDataset{Namespace: ns, Name: d.Name}
		if len(d.Fields) > 0 || (withStats && rowCount >= 0) {
			od.Facets = &olFacets{}
			if len(d.Fields) > 0 {
				fields := make([]olSchemaField, len(d.Fields))
				for i, f := range d.Fields {
					fields[i] = olSchemaField(f)
				}
				od.Facets.Schema = &olSchemaFacet{Producer: producer, SchemaURL: schemaFacetURL, Fields: fields}
			}
			if withStats && rowCount >= 0 {
				od.Facets.OutputStatistics = &olOutStats{Producer: producer, SchemaURL: outStatsFacetURL, RowCount: rowCount}
			}
		}
		out = append(out, od)
	}
	return out
}

// uuidFromString derives a stable RFC-4122 (v5-style) UUID from an arbitrary
// string, so the START and COMPLETE events of one run share the same runId as
// OpenLineage requires, even though the engine's run id is not itself a UUID.
func uuidFromString(s string) string {
	h := sha1.Sum([]byte("flogo-datapipeline:" + s))
	var b [16]byte
	copy(b[:], h[:16])
	b[6] = (b[6] & 0x0f) | 0x50 // version 5
	b[8] = (b[8] & 0x3f) | 0x80 // RFC-4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// --- OpenLineage JSON model ---

type olEvent struct {
	EventType string      `json:"eventType"`
	EventTime string      `json:"eventTime"`
	Producer  string      `json:"producer"`
	SchemaURL string      `json:"schemaURL"`
	Run       olRun       `json:"run"`
	Job       olJob       `json:"job"`
	Inputs    []olDataset `json:"inputs,omitempty"`
	Outputs   []olDataset `json:"outputs,omitempty"`
}

type olRun struct {
	RunID string `json:"runId"`
}

type olJob struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type olDataset struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	Facets    *olFacets `json:"facets,omitempty"`
}

type olFacets struct {
	Schema           *olSchemaFacet `json:"schema,omitempty"`
	OutputStatistics *olOutStats    `json:"outputStatistics,omitempty"`
}

type olSchemaFacet struct {
	Producer  string          `json:"_producer"`
	SchemaURL string          `json:"_schemaURL"`
	Fields    []olSchemaField `json:"fields"`
}

type olSchemaField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type olOutStats struct {
	Producer  string `json:"_producer"`
	SchemaURL string `json:"_schemaURL"`
	RowCount  int64  `json:"rowCount"`
}

// --- transports ---

type lineageSink interface {
	send(b []byte) error
	Close() error
}

// writerSink writes one JSON event per line to an io.Writer (stdout or a file).
type writerSink struct {
	mu     sync.Mutex
	w      io.Writer
	closer io.Closer // non-nil only when we own the writer (a file)
}

func (s *writerSink) send(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.w.Write(append(b, '\n'))
	return err
}

func (s *writerSink) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}

// httpSink POSTs each event to an OpenLineage collector.
type httpSink struct {
	url    string
	client *http.Client
}

func (s *httpSink) send(b []byte) error {
	resp, err := s.client.Post(s.url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("lineage http %s: status %d", s.url, resp.StatusCode)
	}
	return nil
}

func (s *httpSink) Close() error { return nil }
