package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

func TestDrillFlowIsolatedAndComparable(t *testing.T) {
	server := testServer(t)

	runDrill := func() (string, map[string]any) {
		t.Helper()
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/api/routes/R-S3/drills", bytes.NewReader([]byte(`{}`)))
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusCreated {
			t.Fatalf("drill status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		var response map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		run := response["run"].(map[string]any)
		return run["id"].(string), response
	}

	firstID, firstBody := runDrill()
	secondID, secondBody := runDrill()
	if firstID != secondID {
		t.Fatalf("expected identical run IDs for same snapshot, got %s and %s", firstID, secondID)
	}
	if !bytes.Equal(mustJSON(t, firstBody), mustJSON(t, secondBody)) {
		t.Fatal("expected identical drill reports for repeated runs")
	}

	// GET the stored run by its deterministic ID.
	get := httptest.NewRecorder()
	server.Handler().ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/drills/"+firstID, nil))
	if get.Code != http.StatusOK {
		t.Fatalf("get drill status=%d body=%s", get.Code, get.Body.String())
	}

	// Live route and topology must remain on primary after the drills.
	live := httptest.NewRecorder()
	server.Handler().ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/api/routes/R-S3", nil))
	if live.Code != http.StatusOK {
		t.Fatalf("get route status=%d", live.Code)
	}
	var liveRoute map[string]any
	if err := json.Unmarshal(live.Body.Bytes(), &liveRoute); err != nil {
		t.Fatal(err)
	}
	if liveRoute["state"] != string(route.StatePrimary) || liveRoute["active_path"] != string(topology.PathPrimary) {
		t.Fatalf("drill changed live route: %s", live.Body.String())
	}
}

func TestDrillUnknownRoute(t *testing.T) {
	server := testServer(t)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/routes/missing/drills", bytes.NewReader([]byte(`{}`))))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown route, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
