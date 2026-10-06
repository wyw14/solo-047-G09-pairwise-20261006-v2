package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (s *Server) startSwitchover(writer http.ResponseWriter, request *http.Request) {
	routeID := chi.URLParam(request, "id")
	target, err := s.services.Route(routeID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input SwitchoverRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	result, err := s.services.Switchovers.Failover(request.Context(), target)
	// Record the event as a read-only byproduct for later drills. The recorder
	// never feeds back into the switchover path.
	if s.services.SwitchoverEvents != nil {
		s.services.SwitchoverEvents.Record(result)
	}
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("switchover", result.ID, result); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, result)
}

func (s *Server) getRoute(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Route(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}
