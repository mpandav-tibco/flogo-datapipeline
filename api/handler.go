package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
)

// handlePipelinesPrefix is the catch-all handler for all /v1/pipelines/{ref}
// sub-paths. It demultiplexes by method and path suffix.
//
// Registered routes:
//
//	POST   /v1/pipelines/{ref}/run              → handleRunPipeline
//	DELETE /v1/pipelines/{ref}/run/{runId}      → handleCancelRun
//	GET    /v1/pipelines/{ref}/status            → handlePipelineStatus
//	GET    /v1/pipelines/{ref}/definition        → stub (501)
//	PUT    /v1/pipelines/{ref}                   → stub (501)
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

	// DELETE /v1/pipelines/{ref}/run/{runId}
	case len(parts) == 3 && parts[1] == "run" && r.Method == http.MethodDelete:
		s.handleCancelRun(w, r, ref, parts[2])

	// GET /v1/pipelines/{ref}/status
	case len(parts) == 2 && parts[1] == "status" && r.Method == http.MethodGet:
		s.handlePipelineStatus(w, r, ref)

	// GET /v1/pipelines/{ref}/definition — future UI read-back, stubbed
	case len(parts) == 2 && parts[1] == "definition" && r.Method == http.MethodGet:
		writeError(w, http.StatusNotImplemented, "not_implemented",
			"GET /v1/pipelines/{ref}/definition is not yet implemented")

	// PUT /v1/pipelines/{ref} — future UI write-back, stubbed
	case len(parts) == 1 && r.Method == http.MethodPut:
		writeError(w, http.StatusNotImplemented, "not_implemented",
			"PUT /v1/pipelines/{ref} is not yet implemented")

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

	// Build a cancellable child context so the run can be aborted via DELETE.
	runCtx, cancel := context.WithCancel(r.Context())
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
			defer func() { s.cancels.Delete(req.RunID) }()
			out, _ := act.RunDirect(runCtx, in)
			resp := mapOutputToRunResponse(req.RunID, startedAt, out)
			s.ringFor(ref).push(resp)
			s.setStatus(ref, out.Status)
		}()
		return
	}

	// Synchronous: block until completion.
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

// findAction looks up a DataPipelineAction by its resource ref.
// The ref may be supplied as a bare label (e.g. "daily-tx") or as the full
// Flogo resource reference (e.g. "res://pipeline:daily-tx"); both forms are tried.
func (s *Server) findAction(ref string) *pipeline.DataPipelineAction {
	actions := s.factory.Actions()

	// Direct lookup (exact match).
	if act, ok := actions[ref]; ok {
		return act
	}

	// Try the canonical resource reference form.
	canonical := "res://pipeline:" + ref
	if act, ok := actions[canonical]; ok {
		return act
	}

	// Try stripping the "res://pipeline:" prefix if the caller sent the full ref.
	for k, act := range actions {
		if k == ref || strings.TrimPrefix(k, "res://pipeline:") == ref {
			return act
		}
	}
	return nil
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
	return PipelineRunResponse{
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
}
