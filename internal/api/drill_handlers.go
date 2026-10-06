package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/wyw14/cry-103/internal/drill"
)

// runDrill captures the live world read-only and replays a failover inside an
// isolated replica. It never changes routes, topology, alarms or permits, and
// never writes the event journal.
func (s *Server) runDrill(writer http.ResponseWriter, request *http.Request) {
	if s.services.Drills == nil {
		writeError(writer, http.StatusServiceUnavailable, drillUnavailable)
		return
	}
	routeID := chi.URLParam(request, "id")
	params := drill.Parameters{}
	if request.ContentLength != 0 {
		var input DrillRequest
		if err := decodeJSON(request, &input); err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
		if input.DrainTimeoutMS < 0 || input.DrainDelayMS < 0 {
			writeError(writer, http.StatusBadRequest, invalidDrainTimeout)
			return
		}
		params.DrainTimeout = time.Duration(input.DrainTimeoutMS) * time.Millisecond
		params.DrainDelay = time.Duration(input.DrainDelayMS) * time.Millisecond
	}
	snapshot, run, err := s.services.Drills.CaptureAndRun(routeID, params)
	if err != nil {
		if _, ok := err.(*drill.NotFoundError); ok {
			writeError(writer, http.StatusNotFound, err)
			return
		}
		if _, ok := err.(*drill.InvalidSnapshotError); ok {
			writeError(writer, http.StatusConflict, err)
			return
		}
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusCreated, DrillRunResponse{
		SnapshotFingerprint: snapshot.Fingerprint(), Run: run,
	})
}

func (s *Server) getDrillRun(writer http.ResponseWriter, request *http.Request) {
	if s.services.Drills == nil {
		writeError(writer, http.StatusServiceUnavailable, drillUnavailable)
		return
	}
	run, ok := s.services.Drills.GetRun(chi.URLParam(request, "id"))
	if !ok {
		writeError(writer, http.StatusNotFound, &drill.NotFoundError{What: "drill run", ID: chi.URLParam(request, "id")})
		return
	}
	writeJSON(writer, http.StatusOK, run)
}

func (s *Server) getDrillSnapshot(writer http.ResponseWriter, request *http.Request) {
	if s.services.Drills == nil {
		writeError(writer, http.StatusServiceUnavailable, drillUnavailable)
		return
	}
	fingerprint := chi.URLParam(request, "fingerprint")
	snapshot, ok := s.services.Drills.GetSnapshot(fingerprint)
	if !ok {
		writeError(writer, http.StatusNotFound, &drill.NotFoundError{What: "drill snapshot", ID: fingerprint})
		return
	}
	writeJSON(writer, http.StatusOK, snapshot)
}

type sentinelError string

func (e sentinelError) Error() string { return string(e) }

const (
	drillUnavailable    sentinelError = "drill module is not enabled"
	invalidDrainTimeout sentinelError = "drain_timeout_ms must be non-negative"
)
