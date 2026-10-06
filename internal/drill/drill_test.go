package drill

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

func fixture(t *testing.T) (*Service, *topology.Registry, *route.Route, time.Time) {
	t.Helper()
	base := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	clock := func() time.Time { return base }
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, base); err != nil {
		t.Fatal(err)
	}
	path, err := route.New("R-S3", "S3", base)
	if err != nil {
		t.Fatal(err)
	}
	journal := store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl"))
	repository := telemetry.NewRepository()
	service := NewService(registry, map[string]*route.Route{"R-S3": path}, repository, journal, clock)
	return service, registry, path, base
}

func TestRunIsDeterministicAndIsolated(t *testing.T) {
	service, registry, path, _ := fixture(t)

	firstSnapshot, first, err := service.CaptureAndRun("R-S3", Parameters{})
	if err != nil {
		t.Fatal(err)
	}
	// Re-capture from live state and replay again: must address the same run.
	secondSnapshot, second, err := service.CaptureAndRun("R-S3", Parameters{})
	if err != nil {
		t.Fatal(err)
	}

	if first.ID != second.ID {
		t.Fatalf("run IDs differ for identical snapshots: %s vs %s", first.ID, second.ID)
	}
	if firstSnapshot.Fingerprint() != secondSnapshot.Fingerprint() {
		t.Fatalf("fingerprints differ for identical live state: %s vs %s",
			firstSnapshot.Fingerprint(), secondSnapshot.Fingerprint())
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("replay reports differ:\n%s\n%s", firstJSON, secondJSON)
	}

	// Live topology must be untouched: still on primary at generation 1.
	liveTopo, err := registry.Snapshot("S3")
	if err != nil {
		t.Fatal(err)
	}
	if liveTopo.Generation != 1 || liveTopo.ActivePath != topology.PathPrimary {
		t.Fatalf("drill mutated live topology: %+v", liveTopo)
	}
	// Live route must still carry traffic on primary.
	liveRoute := path.Snapshot()
	if liveRoute.State != route.StatePrimary || liveRoute.ActivePath != topology.PathPrimary ||
		liveRoute.TrafficBPS != 1_000_000 {
		t.Fatalf("drill mutated live route: %+v", liveRoute)
	}
	// The replica advanced generations, the live world did not.
	if first.Replay.FinalTopo.Generation <= liveTopo.Generation {
		t.Fatalf("expected replica to advance generation, initial=%d final=%d",
			liveTopo.Generation, first.Replay.FinalTopo.Generation)
	}

	// Drill must never append to the event journal.
	records, err := service.journal.Records()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("drill wrote %d journal records; expected none", len(records))
	}
}

func TestParametersChangeRunIdentity(t *testing.T) {
	service, _, _, _ := fixture(t)
	defaultRun, err := service.Run(mustCapture(t, service), Parameters{})
	if err != nil {
		t.Fatal(err)
	}
	extended, err := service.Run(mustCapture(t, service), Parameters{DrainTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if defaultRun.ID == extended.ID {
		t.Fatalf("different drain budgets must yield different run IDs: %s", defaultRun.ID)
	}
}

func TestSimulatedDrainTimeout(t *testing.T) {
	service, _, _, _ := fixture(t)
	// Simulated drain (100ms) exceeds the default 25ms budget.
	run, err := service.Run(mustCapture(t, service), Parameters{DrainDelay: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if run.Outcome != "protected-with-drain-timeout" {
		t.Fatalf("expected protected-with-drain-timeout, got %s (%+v)", run.Outcome, run.Findings)
	}
	if !hasFinding(run, SeverityRisk, "drain-timeout") {
		t.Fatalf("expected drain-timeout risk, got %+v", run.Findings)
	}
	// Same parameters must reproduce the identical run.
	again, err := service.Run(mustCapture(t, service), Parameters{DrainDelay: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != run.ID {
		t.Fatalf("drain-timeout replay not reproducible: %s vs %s", run.ID, again.ID)
	}
}

func TestDrainWithinBudgetProtectsCleanly(t *testing.T) {
	service, _, _, _ := fixture(t)
	run, err := service.Run(mustCapture(t, service), Parameters{
		DrainTimeout: 100 * time.Millisecond, DrainDelay: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Outcome != "protected" {
		t.Fatalf("expected clean protection, got %s (%+v)", run.Outcome, run.Findings)
	}
}

func TestFailedLastSwitchoverPredictsBlocker(t *testing.T) {
	service, _, _, base := fixture(t)
	failed := switchover.Snapshot{
		ID: "switch-old", RouteID: "R-S3", State: switchover.StateFailed, UpdatedAt: base,
		Events: []switchover.Event{
			{Stage: "primary-drain", Message: "draining failed primary path", At: base},
			{Stage: "protection-activate", Message: "protection path must be open before protection connects", At: base.Add(time.Millisecond)},
		},
	}
	if err := service.journal.Append("switchover", failed.ID, failed, base); err != nil {
		t.Fatal(err)
	}

	_, run, err := service.CaptureAndRun("R-S3", Parameters{})
	if err != nil {
		t.Fatal(err)
	}
	if run.Outcome != "blocked-predicted" {
		t.Fatalf("expected blocked-predicted, got %s (%+v)", run.Outcome, run.Findings)
	}
	if run.BlockingPoint == nil || run.BlockingPoint.Code != "previous-switchover-failed" {
		t.Fatalf("expected recurrence blocker, got %+v", run.BlockingPoint)
	}
	if !hasFinding(run, SeverityBlocker, "previous-switchover-failed") {
		t.Fatalf("missing blocker finding: %+v", run.Findings)
	}
}

func TestUnstableAndStaleTelemetryRaiseRisk(t *testing.T) {
	service, _, _, base := fixture(t)
	// Two pump-current samples with a wide spread near cutover => unstable.
	samples := []telemetry.Sample{
		{ID: "t1", AssetID: "R07", Kind: telemetry.KindCurrent, Value: 1.0, Unit: "A", Sequence: 1, Generation: 1, ObservedAt: base.Add(-2 * time.Second), ReceivedAt: base},
		{ID: "t2", AssetID: "R07", Kind: telemetry.KindCurrent, Value: 5.0, Unit: "A", Sequence: 2, Generation: 1, ObservedAt: base.Add(-1 * time.Second), ReceivedAt: base},
	}
	for _, sample := range samples {
		if err := service.repository.Add(sample); err != nil {
			t.Fatal(err)
		}
	}
	_, run, err := service.CaptureAndRun("R-S3", Parameters{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(run, SeverityRisk, "unstable-repeater-current") {
		t.Fatalf("expected unstable current risk, got %+v", run.Findings)
	}
	if run.Outcome != "protected" {
		t.Fatalf("healthy cutover with risk findings should still protect, got %s", run.Outcome)
	}
}

func TestStaleTelemetryRaisesRisk(t *testing.T) {
	service, _, _, base := fixture(t)
	stale := telemetry.Sample{
		ID: "t1", AssetID: "R07", Kind: telemetry.KindTemperature, Value: 30, Unit: "C",
		Sequence: 1, Generation: 1, ObservedAt: base.Add(-90 * time.Second), ReceivedAt: base.Add(-90 * time.Second),
	}
	if err := service.repository.Add(stale); err != nil {
		t.Fatal(err)
	}
	_, run, err := service.CaptureAndRun("R-S3", Parameters{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(run, SeverityRisk, "stale-telemetry") {
		t.Fatalf("expected stale telemetry risk, got %+v", run.Findings)
	}
}

func TestMissingRoute(t *testing.T) {
	service, _, _, _ := fixture(t)
	if _, _, err := service.CaptureAndRun("nope", Parameters{}); err == nil {
		t.Fatal("expected error for unknown route")
	}
}

func TestGetRunAndSnapshot(t *testing.T) {
	service, _, _, _ := fixture(t)
	snap, run, err := service.CaptureAndRun("R-S3", Parameters{})
	if err != nil {
		t.Fatal(err)
	}
	stored, ok := service.GetRun(run.ID)
	if !ok || stored.ID != run.ID {
		t.Fatal("run not retrievable by deterministic ID")
	}
	captured, ok := service.GetSnapshot(snap.Fingerprint())
	if !ok || captured.RouteID() != "R-S3" {
		t.Fatal("snapshot not retrievable by fingerprint")
	}
}

func mustCapture(t *testing.T, service *Service) Snapshot {
	t.Helper()
	snap, err := service.Capture("R-S3")
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func hasFinding(run Run, severity Severity, code string) bool {
	for _, finding := range run.Findings {
		if finding.Severity == severity && finding.Code == code {
			return true
		}
	}
	return false
}
