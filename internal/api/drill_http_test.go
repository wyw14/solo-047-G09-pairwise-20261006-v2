package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type drillHTTPReport struct {
	ID         string `json:"id"`
	RouteID    string `json:"route_id"`
	SnapshotFP string `json:"snapshot_fingerprint"`
	FP         string `json:"fingerprint"`
	Prediction struct {
		Outcome  string `json:"outcome"`
		Blocking bool   `json:"blocking"`
		Blockers []struct {
			Code string `json:"code"`
		} `json:"blockers"`
		Risks []struct {
			Code string `json:"code"`
		} `json:"risks"`
	} `json:"prediction"`
	Snapshot struct {
		Topology struct {
			Generation uint64 `json:"generation"`
			ActivePath string `json:"active_path"`
		} `json:"topology"`
		Route struct {
			State      string `json:"state"`
			ActivePath string `json:"active_path"`
		} `json:"route"`
		LastEvent *struct {
			ID string `json:"id"`
		} `json:"last_switchover"`
	} `json:"snapshot"`
	Comparison *struct {
		SameSnapshot      bool `json:"same_snapshot"`
		PredictionChanged bool `json:"prediction_changed"`
	} `json:"comparison"`
}

func doDrill(t *testing.T, server *Server, body string) drillHTTPReport {
	t.Helper()
	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/routes/R-S3/switchover-drills", reader)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("drill status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var report drillHTTPReport
	if err := json.Unmarshal(recorder.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	return report
}

func TestSwitchoverDrillEndpointIsIsolated(t *testing.T) {
	server := testServer(t)

	beforeTopo := getJSON(t, server, "/api/topology/S3")
	beforeRoute := getJSON(t, server, "/api/routes/R-S3")
	journalBefore, err := server.services.Journal.Records()
	if err != nil {
		t.Fatal(err)
	}

	report := doDrill(t, server, `{"reason":"pre-cut rehearsal"}`)
	if report.Prediction.Outcome != "protected" || report.Prediction.Blocking {
		t.Fatalf("expected clean protected drill, got outcome=%s blocking=%v blockers=%+v",
			report.Prediction.Outcome, report.Prediction.Blocking, report.Prediction.Blockers)
	}
	if report.Snapshot.LastEvent != nil {
		t.Fatalf("no live switchover has happened, drill must not invent a last event")
	}

	// The drill advanced its clone to protection; the captured snapshot must
	// still show the primary generation, and the live services must be
	// byte-identical.
	if report.Snapshot.Topology.Generation != 1 || report.Snapshot.Topology.ActivePath != "primary" {
		t.Fatalf("captured snapshot must reflect pre-drill state, got gen=%d path=%s",
			report.Snapshot.Topology.Generation, report.Snapshot.Topology.ActivePath)
	}
	afterTopo := getJSON(t, server, "/api/topology/S3")
	afterRoute := getJSON(t, server, "/api/routes/R-S3")
	if string(afterTopo) != string(beforeTopo) || string(afterRoute) != string(beforeRoute) {
		t.Fatalf("drill mutated live state:\ntopo before=%s after=%s\nroute before=%s after=%s",
			beforeTopo, afterTopo, beforeRoute, afterRoute)
	}
	journalAfter, err := server.services.Journal.Records()
	if err != nil {
		t.Fatal(err)
	}
	if len(journalAfter) != len(journalBefore) {
		t.Fatalf("drill appended operational journal events: before=%d after=%d",
			len(journalBefore), len(journalAfter))
	}
}

func TestSwitchoverDrillReplayCompares(t *testing.T) {
	server := testServer(t)
	first := doDrill(t, server, "")

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/switchover-drills/"+first.ID+"/replay", bytes.NewReader([]byte(`{"reason":"rerun"}`)))
	request.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("replay status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var second drillHTTPReport
	if err := json.Unmarshal(recorder.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if second.Comparison == nil || !second.Comparison.SameSnapshot || second.Comparison.PredictionChanged {
		t.Fatalf("replay of the same snapshot must compare unchanged, got %+v", second.Comparison)
	}
	if second.FP != first.FP || second.SnapshotFP != first.SnapshotFP {
		t.Fatalf("replay fingerprints must match the first run")
	}
}

func TestLiveSwitchoverIsVisibleToLaterDrillButDrillStaysReadonly(t *testing.T) {
	server := testServer(t)

	switchRecorder := httptest.NewRecorder()
	switchRequest := httptest.NewRequest(http.MethodPost, "/api/routes/R-S3/switchovers", bytes.NewReader([]byte(`{"reason":"real cut"}`)))
	switchRequest.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(switchRecorder, switchRequest)
	if switchRecorder.Code != http.StatusAccepted {
		t.Fatalf("switchover status=%d body=%s", switchRecorder.Code, switchRecorder.Body.String())
	}

	// Live route is now on protection; a drill must predict a blocker and must
	// not move any state itself.
	beforeTopo := getJSON(t, server, "/api/topology/S3")
	report := doDrill(t, server, "")
	if !report.Prediction.Blocking {
		t.Fatalf("expected blocking drill on protection path, risks=%v blockers=%v",
			report.Prediction.Risks, report.Prediction.Blockers)
	}
	found := false
	for _, blocker := range report.Prediction.Blockers {
		if blocker.Code == "already-on-protection" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected already-on-protection blocker, got %+v", report.Prediction.Blockers)
	}
	if report.Snapshot.LastEvent == nil {
		t.Fatalf("drill must replay the most recent live switchover event")
	}
	afterTopo := getJSON(t, server, "/api/topology/S3")
	if string(afterTopo) != string(beforeTopo) {
		t.Fatalf("blocked drill must not change live topology: before=%s after=%s", beforeTopo, afterTopo)
	}
}

func TestListDrills(t *testing.T) {
	server := testServer(t)
	doDrill(t, server, "")
	doDrill(t, server, "")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/switchover-drills", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 2 {
		t.Fatalf("expected 2 drill reports, got %d", len(payload.Items))
	}
}

func getJSON(t *testing.T, server *Server, path string) []byte {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET %s status=%d", path, recorder.Code)
	}
	return recorder.Body.Bytes()
}
