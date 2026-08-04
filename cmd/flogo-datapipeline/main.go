// Command flogo-datapipeline is a self-contained Flogo app that demonstrates
// the datapipeline: action. All source, sink, transform, checkpoint, watermark,
// and metrics backends are registered via blank imports — each sub-package's
// init() calls Register() against the appropriate registry.
//
// Registered capabilities (see also doc.go for the full type inventory):
//
//	Sources:      csv, jdbc, parquet, fixedwidth, xml, jsonlines, s3, gcs, adls, kafka
//	Sinks:        jdbc, null, parquet, s3-sink, gcs-sink, adls-sink, dlq, kafka
//	Transforms:   filter, map, lookup, validate, schemadrift, hwm, dq, parse, window
//	Checkpoints:  file, redis
//	Watermarks:   file, redis, postgres
//	Metrics:      prometheus, otel
//	ObjStore:     s3, gcs, adls (provider backends shared by source/objstore and sink/objstore)
//
// Pipeline Management REST API:
//
//	Set the PIPELINE_API_ADDR environment variable (or pass --api-addr) to enable
//	the HTTP management API. When enabled, a REST server starts alongside the
//	Flogo engine in a separate goroutine.
//
//	  PIPELINE_API_ADDR=:8888 ./flogo-datapipeline
//	  ./flogo-datapipeline --api-addr :8888
//
//	API endpoints:
//	  GET    /v1/health                         liveness probe
//	  GET    /v1/pipelines                      list registered pipelines
//	  POST   /v1/pipelines/{ref}/run            trigger a pipeline run
//	  GET    /v1/pipelines/{ref}/status         last N run results (ring buffer)
//	  DELETE /v1/pipelines/{ref}/run/{runId}    cancel an in-flight run
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/project-flogo/core/engine"
	corelog "github.com/project-flogo/core/support/log"

	// --- Flogo action (must be first) ---
	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
	"github.com/mpandav-tibco/flogo-datapipeline/api"
	"github.com/mpandav-tibco/flogo-datapipeline/internal/appprops"

	// --- ObjStore provider backends ---
	// These three packages register the concrete S3, GCS, and ADLS Gen2 clients
	// with the objstore registry. They must be imported before source/objstore
	// and sink/objstore, which look up providers by name at pipeline start.
	_ "github.com/mpandav-tibco/flogo-datapipeline/objstore/adls" // Azure Data Lake Storage Gen2
	_ "github.com/mpandav-tibco/flogo-datapipeline/objstore/gcs"  // Google Cloud Storage
	_ "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3"   // AWS S3 (and S3-compatible)

	// --- Sources ---
	// File / relational sources (round 1)
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"  // CSV with optional header row
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jdbc" // JDBC query source (chunked fetch)

	// Additional file formats (gap-08)
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/avro"       // Avro Object Container File (OCF)
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/fixedwidth" // Fixed-width / mainframe flat files
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jsonlines"  // JSON-lines (NDJSON)
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/parquet"    // Parquet with row-group parallelism
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/sectioned"  // Text document: key/value header + delimited body table
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/xml"        // XML streaming (SAX-style, element-per-row)

	// Cloud object-storage source (gap-08); activates s3, gcs, adls source types
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/objstore"

	// Streaming sources (gap-06)
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/kafka" // Kafka consumer with consumer-group offset checkpointing

	// Kubernetes pod-log collector (Fluentd replacement) — tails CRI container
	// logs, stitches multiline stack traces, at-least-once via a byte-offset
	// position registry. See examples/20-podlog.
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/podlog"

	// --- Sinks ---
	// Relational sinks (round 1)
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc" // JDBC upsert / insert sink
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/null" // Null sink (benchmarking / dry-run)

	// Columnar / binary file sinks (gap-08)
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/avro"    // Avro Object Container File (OCF) sink
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/parquet" // Parquet row-group-aligned file sink

	// Text file sinks — used directly and as object-store inner formats (csv/jsonlines).
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/csv"       // CSV / TSV delimited-text file sink
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines" // JSON-lines (NDJSON) file sink
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/template"  // Custom text: render rows through a Go text/template

	// Cloud object-storage sink (gap-08); activates s3-sink, gcs-sink, adls-sink types
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/objstore"

	// Streaming sinks (gap-06)
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/kafka" // Kafka producer with exactly-once semantics via transactions

	// Dead-letter queue sink (gap-07): write-time row isolation + DLQ routing.
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/dlq"

	// --- Transforms ---
	// Core transforms (round 1)
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter" // Row-level boolean filter
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/mapper" // Field projection / rename / derive

	// Additional transforms (gap-08)
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/cast"        // Column type casting
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/dq"          // Data-quality expectations with pass-rate thresholds
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/explode"     // Expand an array column into one row per element
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/flatten"     // Flatten nested-object columns into dotted columns
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/hwm"         // High-water-mark observer (pure passthrough)
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/join"        // Keyed join against a source-loaded reference table (stream-table)
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/lookup"      // Enrichment via LRU-cached flow/HTTP call
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/parse"       // Structure a text field into columns (regex/JSON) — Fluent Bit parser equivalent
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/router"      // Conditional router / split — N-way route rows to named sinks by predicate
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/schemadrift" // Schema-drift detection and remediation
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/union"       // UNION ALL — append bounded secondary sources to the stream (N→1)
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/validate"    // Data-quality rules; violations routed to DLQ sink
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/window"      // Event-time windowed aggregation (tumbling/sliding/session/global)

	// --- Checkpoints ---
	_ "github.com/mpandav-tibco/flogo-datapipeline/checkpoint/file"  // Local-file checkpoint (default)
	_ "github.com/mpandav-tibco/flogo-datapipeline/checkpoint/redis" // Redis checkpoint for multi-instance pipelines

	// --- Watermarks ---
	// file watermark is the built-in default; redis and postgres add HA backends.
	_ "github.com/mpandav-tibco/flogo-datapipeline/watermark/file"     // File-backed watermark store (default)
	_ "github.com/mpandav-tibco/flogo-datapipeline/watermark/postgres" // PostgreSQL-backed watermark store (upsert)
	_ "github.com/mpandav-tibco/flogo-datapipeline/watermark/redis"    // Redis-backed watermark store

	// --- Metrics backends ---
	_ "github.com/mpandav-tibco/flogo-datapipeline/metrics/otel"       // OpenTelemetry OTLP gRPC exporter
	_ "github.com/mpandav-tibco/flogo-datapipeline/metrics/prometheus" // Prometheus /metrics scrape endpoint

	// --- Database drivers ---
	// Import whichever drivers your deployment uses; unused drivers are trimmed
	// by the linker. All four are pure Go, so the binary builds without CGO.
	_ "github.com/go-sql-driver/mysql"  // MySQL / MariaDB (pure Go)
	_ "github.com/lib/pq"               // PostgreSQL (pure Go)
	_ "github.com/microsoft/go-mssqldb" // SQL Server (pure Go)
	_ "github.com/sijms/go-ora/v2"      // Oracle (pure Go)

	// --- Flogo trigger (timer fires the pipeline on a schedule) ---
	_ "github.com/project-flogo/contrib/trigger/timer"
	// REST trigger — lets a deployed app expose an HTTP endpoint that invokes the
	// pipeline. Selectable from the Studio's Pipeline Settings → Trigger tab and
	// materialised into the deploy app JSON. Inert unless referenced by a trigger.
	_ "github.com/project-flogo/contrib/trigger/rest"
	// Kafka trigger — run a pipeline per consumed message (event-driven ingestion).
	_ "github.com/project-flogo/messaging-contrib/kafka/trigger"
	// TIBCO General hooks — App Startup fires the pipeline once on engine boot
	// (the correct driver for a long-running streaming source such as the podlog
	// tailer); App Shutdown fires once on stop for final flush/cleanup.
	_ "github.com/tibco/flogo-general/src/app/General/trigger/shutdownhook"
	_ "github.com/tibco/flogo-general/src/app/General/trigger/startuphook"
)

var mlog = corelog.ChildLogger(corelog.RootLogger(), "main")

func main() {
	// -f <path>    path to the .datapipeline file (a flogo:app JSON) — same pattern as flogo-agent
	// -port <n>    API server port (set by the VSCode extension binary manager)
	// -api-addr    full TCP addr override (e.g. :8888); -port takes precedence if both set
	appFile := flag.String("f", "", "Path to the .datapipeline app file (required)")
	apiPort := flag.Int("port", 0, "API server port (e.g. 9080)")
	apiAddr := flag.String("api-addr", "", "API server TCP address (e.g. :8888); overridden by -port")
	flag.Parse()

	if *appFile == "" {
		*appFile = os.Getenv("PIPELINE_FILE")
	}
	if *appFile == "" {
		fmt.Fprintln(os.Stderr, "flogo-datapipeline: -f <path> is required")
		fmt.Fprintln(os.Stderr, "Usage: flogo-datapipeline -f my-pipeline.datapipeline [-port 9080]")
		os.Exit(1)
	}

	raw, err := os.ReadFile(*appFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "flogo-datapipeline: read %s: %v\n", *appFile, err)
		os.Exit(1)
	}
	appJSON := string(raw)

	// Resolve Flogo app-property references ($property[name]) against the app's
	// declared properties[] with env-override precedence (env var >
	// FLOGO_APP_PROPS_OVERRIDE > FLOGO_APP_PROPS_ENV > FLOGO_APP_PROPS_JSON >
	// default value). Lets one .datapipeline run per-environment via env vars /
	// ConfigMaps, matching the flogo-agents / Flogo app-property model.
	appJSON = appprops.Resolve(appJSON)

	// -port wins over -api-addr and env var
	if *apiPort > 0 {
		*apiAddr = fmt.Sprintf(":%d", *apiPort)
	}
	if *apiAddr == "" {
		*apiAddr = os.Getenv("PIPELINE_API_ADDR")
	}

	cfg, err := engine.LoadAppConfig(appJSON, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "flogo-datapipeline: load app config: %v\n", err)
		os.Exit(1)
	}
	e, err := engine.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "flogo-datapipeline: create engine: %v\n", err)
		os.Exit(1)
	}

	if err := e.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "flogo-datapipeline: start engine: %v\n", err)
		os.Exit(1)
	}

	// Start the REST API server when an address is configured.
	// The factory is guaranteed to be non-nil after engine.Start() because
	// Factory.Initialize() is called during action registration inside Start().
	var apiSrv *api.Server
	if *apiAddr != "" {
		f := pipeline.GlobalFactory()
		if f == nil {
			mlog.Warn("pipeline API: factory not available (no pipeline action registered?); API server not started")
		} else {
			apiSrv = api.NewServer(f, *apiAddr)
			go func() {
				if err := apiSrv.Start(); err != nil {
					mlog.Errorf("pipeline API server error: %v", err)
				}
			}()
			mlog.Infof("pipeline API server started on %s", *apiAddr)
		}
	}

	// Wait for a termination signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	<-sigCh

	// Graceful shutdown: stop the API server first (so in-flight HTTP requests
	// can complete), then stop the engine.
	if apiSrv != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := apiSrv.Stop(shutCtx); err != nil {
			mlog.Warnf("pipeline API server shutdown error: %v", err)
		}
	}

	// e.Stop() shuts down the registered connection managers. The project-flogo
	// kafka contrib registers (in init) a placeholder shared-connection manager
	// with a nil connection whose Stop() dereferences it
	// (messaging-contrib/kafka/connection/connection.go:57), so core's App.Stop()
	// panics on EVERY shutdown once the kafka trigger is imported. The pipeline has
	// already finished by this point, so recover from that known third-party panic
	// and exit cleanly instead of crashing the process (SIGSEGV).
	stopErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				mlog.Warnf("recovered during engine shutdown (known kafka contrib nil-connection Stop bug): %v", r)
			}
		}()
		return e.Stop()
	}()
	if stopErr != nil {
		mlog.Errorf("engine stop error: %v", stopErr)
		os.Exit(1)
	}
	os.Exit(0)
}
