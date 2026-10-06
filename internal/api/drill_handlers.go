package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/wyw14/cry-103/internal/drill"
)

type drillEnvelope struct {
	drill.Report
	Comparison *DrillComparison `json:"comparison,omitempty"`
}

// startDrill captures the current state of a route and replays a failover in an
// isolated copy. It never writes to the live topology, route, alarm admission
// or permit services and appends nothing to the operational journal.
func (s *Server) startDrill(writer http.ResponseWriter, request *http.Request) {
	if s.services.Drills == nil {
		writeJSON(writer, http.StatusServiceUnavailable, ErrorResponse{Error: "switchover drills are not configured"})
		return
	}
	target, err := s.services.Route(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	input := DrillRequest{}
	if request.ContentLength != 0 {
		if err := decodeJSON(request, &input); err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
	}
	report, err := s.services.Drills.Run(request.Context(), target, input.Reason)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusCreated, drillEnvelope{Report: report})
}

// replayDrill reruns a prediction against an earlier drill's snapshot. Because
// the replay engine is deterministic, the same snapshot yields the same
// prediction fingerprint; the comparison object makes that explicit.
func (s *Server) replayDrill(writer http.ResponseWriter, request *http.Request) {
	if s.services.Drills == nil {
		writeJSON(writer, http.StatusServiceUnavailable, ErrorResponse{Error: "switchover drills are not configured"})
		return
	}
	input := DrillRequest{}
	if request.ContentLength != 0 {
		if err := decodeJSON(request, &input); err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
	}
	base, exists := s.services.Drills.Get(chi.URLParam(request, "drillID"))
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "drill not found"})
		return
	}
	report, err := s.services.Drills.Replay(request.Context(), base.ID, input.Reason)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusCreated, drillEnvelope{
		Report: report,
		Comparison: &DrillComparison{
			ReportID: report.ID, BaseReportID: base.ID,
			SnapshotFingerprint:       report.SnapshotFingerprint,
			PredictionFingerprint:     report.Fingerprint,
			BasePredictionFingerprint: base.Fingerprint,
			SameSnapshot:              report.SnapshotFingerprint == base.SnapshotFingerprint,
			Comparable:                true,
			PredictionChanged:         report.Fingerprint != base.Fingerprint,
			ComparedAt:                s.services.Clock.Now().UTC(),
		},
	})
}

func (s *Server) getDrill(writer http.ResponseWriter, request *http.Request) {
	if s.services.Drills == nil {
		writeJSON(writer, http.StatusServiceUnavailable, ErrorResponse{Error: "switchover drills are not configured"})
		return
	}
	report, exists := s.services.Drills.Get(chi.URLParam(request, "drillID"))
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "drill not found"})
		return
	}
	writeJSON(writer, http.StatusOK, drillEnvelope{Report: report})
}

func (s *Server) listDrills(writer http.ResponseWriter, request *http.Request) {
	if s.services.Drills == nil {
		writeJSON(writer, http.StatusServiceUnavailable, ErrorResponse{Error: "switchover drills are not configured"})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.services.Drills.List()})
}
