package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline/graphexec"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// handlePipelinesPrefix is the catch-all handler for all /v1/pipelines/{ref}
// sub-paths. It demultiplexes by method and path suffix.
//
// Registered routes:
//
//	POST   /v1/pipelines/{ref}/run              → handleRunPipeline
//	POST   /v1/pipelines/{ref}/preview          → handlePreview
//	POST   /v1/pipelines/{ref}/sampleinput      → handleSampleInput
//	POST   /v1/pipelines/{ref}/teststage        → handleTestStage
//	DELETE /v1/pipelines/{ref}/run/{runId}      → handleCancelRun
//	GET    /v1/pipelines/{ref}/status            → handlePipelineStatus
//	GET    /v1/pipelines/{ref}/definition        → handlePipelineDefinition
//	PUT    /v1/pipelines/{ref}                   → handleUpdatePipeline
func (s *Server) handlePipelinesPrefix(w http.ResponseWriter, r *http.Request) {
	// Strip the /v1/pipelines/ prefix and split the remainder.
	tail := strings.TrimPrefix(r.URL.Path, "/v1/pipelines/")
	parts := strings.Split(strings.Trim(tail, "/"), "/")

	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "not_found", "no pipeline ref in path")
		return
	}

	ref := parts[0]

	switch {
	// POST /v1/pipelines/{ref}/run
	case len(parts) == 2 && parts[1] == "run" && r.Method == http.MethodPost:
		s.handleRunPipeline(w, r, ref)

	// POST /v1/pipelines/{ref}/preview
	case len(parts) == 2 && parts[1] == "preview" && r.Method == http.MethodPost:
		s.handlePreview(w, r, ref)

	// POST /v1/pipelines/{ref}/teststage
	case len(parts) == 2 && parts[1] == "teststage" && r.Method == http.MethodPost:
		s.handleTestStage(w, r, ref)

	// POST /v1/pipelines/{ref}/sampleinput
	case len(parts) == 2 && parts[1] == "sampleinput" && r.Method == http.MethodPost:
		s.handleSampleInput(w, r, ref)

	// DELETE /v1/pipelines/{ref}/run/{runId}
	case len(parts) == 3 && parts[1] == "run" && r.Method == http.MethodDelete:
		s.handleCancelRun(w, r, ref, parts[2])

	// GET /v1/pipelines/{ref}/status
	case len(parts) == 2 && parts[1] == "status" && r.Method == http.MethodGet:
		s.handlePipelineStatus(w, r, ref)

	// GET /v1/pipelines/{ref}/definition
	case len(parts) == 2 && parts[1] == "definition" && r.Method == http.MethodGet:
		s.handlePipelineDefinition(w, r, ref)

	// PUT /v1/pipelines/{ref} — runtime create/replace of a pipeline definition
	case len(parts) == 1 && r.Method == http.MethodPut:
		s.handleUpdatePipeline(w, r, ref)

	default:
		writeError(w, http.StatusNotFound, "not_found",
			"route not found: "+r.Method+" "+r.URL.Path)
	}
}

// handleHealth handles GET /v1/health.
// Returns 200 OK with a JSON body so liveness probes can be configured either
// as HTTP GET (body validation) or plain TCP (connection success).
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "GET required")
		return
	}
	actions := s.factory.Actions()
	writeJSON(w, http.StatusOK, HealthResponse{
		Status:    "ok",
		Pipelines: len(actions),
	})
}

// handleListPipelines handles GET /v1/pipelines.
// Returns a snapshot of all registered pipeline actions with their last-run metadata.
func (s *Server) handleListPipelines(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "GET required")
		return
	}

	actions := s.factory.Actions()
	infos := make([]PipelineInfo, 0, len(actions))
	for ref, act := range actions {
		info := buildPipelineInfo(ref, act, s)
		infos = append(infos, info)
	}
	writeJSON(w, http.StatusOK, PipelineListResponse{Pipelines: infos})
}

// handleRunPipeline handles POST /v1/pipelines/{ref}/run.
//
// Query parameters:
//
//	async=true   — return immediately with status "running" (202 Accepted);
//	               the result is stored in the ring buffer and retrievable via
//	               GET /v1/pipelines/{ref}/status.
//	async=false  — (default) block until the run completes and return the full result.
func (s *Server) handleRunPipeline(w http.ResponseWriter, r *http.Request, ref string) {
	act := s.findAction(ref)
	if act == nil {
		writeError(w, http.StatusNotFound, "not_found",
			"no pipeline registered for ref: "+ref)
		return
	}

	var req PipelineRunRequest
	if r.Body != nil && r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request",
				"invalid request body: "+err.Error())
			return
		}
	}

	if req.RunID == "" {
		req.RunID = newRunID()
	}

	in := &pipeline.Input{
		RunID:     req.RunID,
		Params:    req.Params,
		InputPath: req.InputPath,
	}

	async := strings.EqualFold(r.URL.Query().Get("async"), "true")

	// Build a cancellable context for this run.
	// Async runs must use context.Background() as the parent — net/http cancels
	// r.Context() as soon as the handler returns (immediately after the 202), which
	// would kill the goroutine before it processes a single row.
	var runCtx context.Context
	var cancel context.CancelFunc
	if async {
		runCtx, cancel = context.WithCancel(context.Background())
	} else {
		runCtx, cancel = context.WithCancel(r.Context())
	}
	s.cancels.Store(req.RunID, &cancelEntry{cancel: cancel, ref: ref})

	startedAt := time.Now().UTC()
	s.setStatus(ref, "running")

	if async {
		// Fire and forget; respond immediately with 202.
		resp := PipelineRunResponse{
			RunID:     req.RunID,
			Status:    "running",
			StartedAt: startedAt,
		}
		writeJSON(w, http.StatusAccepted, resp)

		go func() {
			defer cancel()
			defer func() { s.cancels.Delete(req.RunID) }()
			out, _ := act.RunDirect(runCtx, in)
			resp := mapOutputToRunResponse(req.RunID, startedAt, out)
			s.ringFor(ref).push(resp)
			s.setStatus(ref, out.Status)
		}()
		return
	}

	// Synchronous: block until completion.
	defer cancel()
	defer func() { s.cancels.Delete(req.RunID) }()
	out, _ := act.RunDirect(runCtx, in)
	resp := mapOutputToRunResponse(req.RunID, startedAt, out)
	s.ringFor(ref).push(resp)
	s.setStatus(ref, out.Status)

	status := http.StatusOK
	if out.Status == "failed" {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, resp)
}

// handleCancelRun handles DELETE /v1/pipelines/{ref}/run/{runId}.
// It cancels the context of the identified in-flight run.
func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request, ref, runID string) {
	v, ok := s.cancels.Load(runID)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found",
			"no in-flight run with id: "+runID)
		return
	}
	entry := v.(*cancelEntry)
	if entry.ref != ref {
		writeError(w, http.StatusNotFound, "not_found",
			"run "+runID+" does not belong to pipeline "+ref)
		return
	}
	entry.cancel()
	s.cancels.Delete(runID)
	s.setStatus(ref, "cancelled")

	// Store a cancelled run record in the ring buffer.
	cancelled := PipelineRunResponse{
		RunID:      runID,
		Status:     "cancelled",
		StartedAt:  time.Now().UTC(), // approximate; we don't store the start time here
		FinishedAt: time.Now().UTC(),
	}
	s.ringFor(ref).push(cancelled)

	w.WriteHeader(http.StatusNoContent)
}

// handlePipelineStatus handles GET /v1/pipelines/{ref}/status.
// Returns the PipelineInfo plus the last N run records from the ring buffer.
func (s *Server) handlePipelineStatus(w http.ResponseWriter, r *http.Request, ref string) {
	act := s.findAction(ref)
	if act == nil {
		writeError(w, http.StatusNotFound, "not_found",
			"no pipeline registered for ref: "+ref)
		return
	}

	info := buildPipelineInfo(ref, act, s)
	runs := s.ringFor(ref).snapshot()

	writeJSON(w, http.StatusOK, PipelineStatusResponse{
		PipelineInfo: info,
		Runs:         runs,
	})
}

// handlePipelineDefinition handles GET /v1/pipelines/{ref}/definition.
// Returns a JSON-safe snapshot of the parsed pipeline definition so the Studio
// can verify what the engine actually loaded (source/sink/transform types).
func (s *Server) handlePipelineDefinition(w http.ResponseWriter, r *http.Request, ref string) {
	act := s.findAction(ref)
	if act == nil {
		writeError(w, http.StatusNotFound, "not_found",
			"no pipeline registered for ref: "+ref)
		return
	}
	writeJSON(w, http.StatusOK, act.DefinitionSummary())
}

// handlePreview handles POST /v1/pipelines/{ref}/preview.
//
// Query parameters:
//
//	limit=<n>   — maximum number of sample rows to return (default 100, max 1000).
//
// It runs a bounded, side-effect-free read+transform sample (no sink writes)
// and returns the resulting rows. Used by the Studio "Dry-run Preview" panel.
func (s *Server) handlePreview(w http.ResponseWriter, r *http.Request, ref string) {
	act := s.findAction(ref)
	if act == nil {
		writeError(w, http.StatusNotFound, "not_found",
			"no pipeline registered for ref: "+ref)
		return
	}

	limit := 100
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 1000 {
		limit = 1000
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	res, err := act.Preview(ctx, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "preview_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSampleInput handles POST /v1/pipelines/{ref}/sampleinput.
//
// Body: { "stage": <int>, "limit": <int> }
//
// Returns a bounded sample of the rows that ENTER the user transform at `stage`
// (i.e. the source run through all preceding transforms). Seeds the Studio
// atomic stage tester with realistic upstream data.
func (s *Server) handleSampleInput(w http.ResponseWriter, r *http.Request, ref string) {
	act := s.findAction(ref)
	if act == nil {
		writeError(w, http.StatusNotFound, "not_found", "no pipeline registered for ref: "+ref)
		return
	}
	var body struct {
		Stage int `json:"stage"`
		Limit int `json:"limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error())
		return
	}
	limit := body.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	res, err := act.SampleInput(ctx, limit, body.Stage)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "sample_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleTestStage handles POST /v1/pipelines/{ref}/teststage.
//
// Body: { "stage": <int>, "columns": ["a","b"], "rows": [ {"a":1,"b":2}, … ] }
//
// Runs the single transform at `stage` against the supplied input rows in
// isolation and returns per-row kept/dropped/failed outcomes. Side-effect-free.
func (s *Server) handleTestStage(w http.ResponseWriter, r *http.Request, ref string) {
	act := s.findAction(ref)
	if act == nil {
		writeError(w, http.StatusNotFound, "not_found", "no pipeline registered for ref: "+ref)
		return
	}
	var body struct {
		Stage   int                      `json:"stage"`
		Columns []string                 `json:"columns"`
		Rows    []map[string]interface{} `json:"rows"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error())
		return
	}
	if len(body.Rows) == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "no input rows supplied")
		return
	}
	if len(body.Rows) > 200 {
		body.Rows = body.Rows[:200]
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	res, err := act.TestStage(ctx, body.Stage, body.Columns, body.Rows)
	if err != nil {
		writeError(w, http.StatusBadRequest, "teststage_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// maxDefinitionBytes bounds the PUT /v1/pipelines/{ref} request body so a
// malformed or hostile payload cannot exhaust memory. Pipeline definitions are
// small JSON documents; 8 MiB is far above any realistic definition.
const maxDefinitionBytes = 8 << 20

// handleUpdatePipeline handles PUT /v1/pipelines/{ref}.
//
// The request body is a pipeline Definition — the same "data" object a
// .datapipeline resource carries (source / transforms / sink / observability /
// graph …). The definition is parsed, defaulted and validated, then atomically
// swapped into the action registry so subsequent runs use it without an engine
// restart. When ref is not yet registered the pipeline is created (201 Created);
// otherwise the existing definition is replaced (200 OK). In-flight runs finish
// on the previous definition. The response is the updated DefinitionSummary,
// exactly as GET /v1/pipelines/{ref}/definition would return it.
func (s *Server) handleUpdatePipeline(w http.ResponseWriter, r *http.Request, ref string) {
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, "bad_request", "request body is required")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxDefinitionBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "read request body: "+err.Error())
		return
	}
	if len(raw) == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "request body is required")
		return
	}

	def := &pipeline.Definition{}
	if err := json.Unmarshal(raw, def); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request",
			"invalid pipeline definition: "+err.Error())
		return
	}
	def.Defaults()
	if err := def.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_definition", err.Error())
		return
	}

	// Resolve the exact registry key so a replace targets the same entry GET/run
	// resolve; a brand-new pipeline is created under the canonical ref form.
	key, existed := resolveActionKey(s.factory.Actions(), ref)
	if !existed {
		key = "res://pipeline:" + ref
	}

	act, err := s.factory.Replace(key, def)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "update_failed", err.Error())
		return
	}

	status := http.StatusOK
	if !existed {
		status = http.StatusCreated
	}
	writeJSON(w, status, act.DefinitionSummary())
}

// findAction looks up a DataPipelineAction by its resource ref.
// The ref may be supplied as a bare label (e.g. "daily-tx") or as the full
// Flogo resource reference (e.g. "res://pipeline:daily-tx"); both forms are tried.
func (s *Server) findAction(ref string) *pipeline.DataPipelineAction {
	actions := s.factory.Actions()
	if key, ok := resolveActionKey(actions, ref); ok {
		return actions[key]
	}
	return nil
}

// resolveActionKey resolves a caller-supplied ref (a bare label like "daily-tx"
// or the full "res://pipeline:daily-tx" reference) to the actual registry key of
// a registered action, reporting whether one exists.
func resolveActionKey(actions map[string]*pipeline.DataPipelineAction, ref string) (string, bool) {
	if _, ok := actions[ref]; ok {
		return ref, true
	}
	canonical := "res://pipeline:" + ref
	if _, ok := actions[canonical]; ok {
		return canonical, true
	}
	for k := range actions {
		if strings.TrimPrefix(k, "res://pipeline:") == ref {
			return k, true
		}
	}
	return "", false
}

// buildPipelineInfo creates a PipelineInfo for the given action, populating
// last-run metadata from the server's ring buffer and status map.
func buildPipelineInfo(ref string, act *pipeline.DataPipelineAction, s *Server) PipelineInfo {
	info := PipelineInfo{
		Ref:        ref,
		SourceType: act.SourceType(),
		SinkType:   act.SinkType(),
		Status:     s.getStatus(ref),
	}

	if rb, ok := func() (*ringBuffer, bool) {
		s.ringsMu.RLock()
		rb, ok := s.rings[ref]
		s.ringsMu.RUnlock()
		return rb, ok
	}(); ok {
		if last, ok := rb.latest(); ok {
			info.LastRunAt = last.StartedAt
			info.LastRunDurationMs = last.DurationMs
			info.LastRunStatus = last.Status
		}
	}
	return info
}

// mapOutputToRunResponse converts a pipeline.Output to a PipelineRunResponse.
func mapOutputToRunResponse(runID string, startedAt time.Time, out *pipeline.Output) PipelineRunResponse {
	resp := PipelineRunResponse{
		RunID:         runID,
		Status:        out.Status,
		RowsProcessed: out.RowsProcessed,
		RowsWritten:   out.RowsWritten,
		RowsFailed:    out.RowsFailed,
		RowsDropped:   out.RowsDropped,
		DurationMs:    out.DurationMs,
		Error:         out.Error,
		StartedAt:     startedAt,
		FinishedAt:    startedAt.Add(time.Duration(out.DurationMs) * time.Millisecond),
	}
	// Surface per-stage row counters (single-path runs) for the graph run overlay.
	if out.Metadata != nil {
		if ss, ok := out.Metadata["stageStats"].([]transform.StageStat); ok {
			resp.StageStats = make([]StageRowStat, len(ss))
			for i, s := range ss {
				resp.StageStats[i] = StageRowStat{Name: s.Name, RowsIn: s.RowsIn, RowsOut: s.RowsOut}
			}
		}
		if gs, ok := out.Metadata["graphStats"].([]graphexec.NodeStat); ok {
			resp.GraphStats = make([]GraphNodeStat, len(gs))
			for i, s := range gs {
				resp.GraphStats[i] = GraphNodeStat{ID: s.ID, Kind: string(s.Kind), RowsIn: s.RowsIn, RowsOut: s.RowsOut, RowsWritten: s.RowsWritten, RowsErrored: s.RowsErrored}
			}
		}
	}
	return resp
}
