package drill

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

var testBase = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)

type fakeRoute struct {
	snap route.Snapshot
}

func (f fakeRoute) Snapshot() route.Snapshot { return f.snap }

func harness(t *testing.T, topo topology.Snapshot, routeSnap route.Snapshot, capturedAt time.Time) (*topology.Registry, *telemetry.Repository, *SwitchoverRecorder, *Captor) {
	t.Helper()
	registry := topology.NewRegistry()
	if err := registry.Load(topo); err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return capturedAt }
	recorder := NewSwitchoverRecorder()
	repository := telemetry.NewRepository()
	captor := NewCaptor(registry, repository, recorder, DefaultThresholds(), clock)
	return registry, repository, recorder, captor
}

func primarySnapshot() (topology.Snapshot, route.Snapshot) {
	topo := topology.Snapshot{
		SpanID: "S3", Generation: 1, ActivePath: topology.PathPrimary,
		ChangedAt: testBase, PrimaryOpen: false, ProtectOpen: true,
	}
	r := route.Snapshot{
		ID: "R-S3", SpanID: "S3", State: route.StatePrimary,
		ActivePath: topology.PathPrimary, TrafficBPS: 1_000_000, UpdatedAt: testBase,
	}
	return topo, r
}

func TestPredictHealthyFailoverAndStageShape(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	_, repository, _, captor := harness(t, topo, routeSnap, testBase.Add(time.Second))
	if err := repository.Add(telemetry.Sample{
		ID: "s1", AssetID: "R07", Kind: telemetry.KindTemperature, Value: 42,
		Sequence: 1, ObservedAt: testBase, ReceivedAt: testBase,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	prediction := NewReplayer().Predict(context.Background(), snapshot)
	if prediction.Blocking || prediction.Outcome != "protected" {
		t.Fatalf("expected unblocked protected prediction, got %+v", prediction)
	}
	if len(prediction.Blockers) != 0 || len(prediction.Risks) != 0 {
		t.Fatalf("expected no findings, got blockers=%v risks=%v", prediction.Blockers, prediction.Risks)
	}
	if prediction.FinalActivePath != string(topology.PathProtection) {
		t.Fatalf("expected final path protection, got %s", prediction.FinalActivePath)
	}
	if prediction.FinalGeneration != topo.Generation+2 {
		t.Fatalf("expected generation %d after open/connect, got %d", topo.Generation+2, prediction.FinalGeneration)
	}
	if prediction.FinalState != string(switchover.StateProtected) {
		t.Fatalf("expected protected operation state, got %s", prediction.FinalState)
	}
	if len(prediction.Stages) != 3 {
		t.Fatalf("expected drain/activate/complete stages, got %+v", prediction.Stages)
	}
	if prediction.DrainElapsedNS != DefaultThresholds().NominalDrain.Nanoseconds() {
		t.Fatalf("expected nominal drain elapsed, got %d", prediction.DrainElapsedNS)
	}
}

func TestDrillDoesNotMutateLiveState(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	registry, _, _, captor := harness(t, topo, routeSnap, testBase)
	liveRoute, err := route.NewFromSnapshot(routeSnap)
	if err != nil {
		t.Fatal(err)
	}
	beforeTopo, _ := registry.Snapshot("S3")
	beforeRoute := liveRoute.Snapshot()

	snapshot, err := captor.Capture(liveRoute)
	if err != nil {
		t.Fatal(err)
	}
	NewReplayer().Predict(context.Background(), snapshot)

	afterTopo, _ := registry.Snapshot("S3")
	afterRoute := liveRoute.Snapshot()
	if afterTopo != beforeTopo {
		t.Fatalf("live topology changed: before=%+v after=%+v", beforeTopo, afterTopo)
	}
	if afterRoute != beforeRoute {
		t.Fatalf("live route changed: before=%+v after=%+v", beforeRoute, afterRoute)
	}
}

func TestCapturedSnapshotIsDetachedFromLiveTelemetry(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	_, repository, _, captor := harness(t, topo, routeSnap, testBase.Add(time.Second))
	if err := repository.Add(telemetry.Sample{
		ID: "s1", AssetID: "R07", Kind: telemetry.KindTemperature, Value: 40,
		Sequence: 1, ObservedAt: testBase,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Add(telemetry.Sample{
		ID: "s2", AssetID: "R07", Kind: telemetry.KindTemperature, Value: 99,
		Sequence: 2, ObservedAt: testBase.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Telemetry["R07"]) != 1 {
		t.Fatalf("snapshot telemetry mutated by live writes: %+v", snapshot.Telemetry["R07"])
	}
}

func TestBlockerWhenAlreadyOnProtection(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	topo.ActivePath = topology.PathProtection
	topo.PrimaryOpen = true
	topo.ProtectOpen = false
	routeSnap.ActivePath = topology.PathProtection
	routeSnap.State = route.StateProtectionActive
	_, _, _, captor := harness(t, topo, routeSnap, testBase)
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	prediction := NewReplayer().Predict(context.Background(), snapshot)
	if !prediction.Blocking || prediction.Outcome != "blocked" {
		t.Fatalf("expected blocked prediction, got %+v", prediction)
	}
	if !containsCode(prediction.Blockers, "already-on-protection") {
		t.Fatalf("expected already-on-protection blocker, got %+v", prediction.Blockers)
	}
	if len(prediction.Stages) != 0 {
		t.Fatalf("blocked prediction must not replay stages, got %+v", prediction.Stages)
	}
	if prediction.FinalGeneration != topo.Generation {
		t.Fatalf("blocked prediction must not advance generation, got %d", prediction.FinalGeneration)
	}
}

func TestBlockerOnPathDivergence(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	topo.ActivePath = topology.PathProtection
	topo.PrimaryOpen = true
	topo.ProtectOpen = false
	_, _, _, captor := harness(t, topo, routeSnap, testBase)
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	prediction := NewReplayer().Predict(context.Background(), snapshot)
	if !containsCode(prediction.Blockers, "path-divergence") {
		t.Fatalf("expected path-divergence blocker, got %+v", prediction.Blockers)
	}
	if containsCode(prediction.Blockers, "already-on-protection") {
		t.Fatalf("route is primary; already-on-protection must not fire")
	}
}

func TestRepeaterTemperatureRisk(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	_, repository, _, captor := harness(t, topo, routeSnap, testBase.Add(time.Second))
	if err := repository.Add(telemetry.Sample{
		ID: "s1", AssetID: "R07", Kind: telemetry.KindTemperature, Value: 87.5,
		Sequence: 1, ObservedAt: testBase,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	prediction := NewReplayer().Predict(context.Background(), snapshot)
	if prediction.Blocking {
		t.Fatalf("temperature is a risk, not a blocker: %+v", prediction.Blockers)
	}
	risk := findFinding(prediction.Risks, "repeater-tripped")
	if risk == nil || risk.Severity != SeverityHigh || risk.AssetID != "R07" {
		t.Fatalf("expected high repeater-tripped risk for R07, got %+v", prediction.Risks)
	}
}

func TestStaleTelemetryRisk(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	_, repository, _, captor := harness(t, topo, routeSnap, testBase.Add(30*time.Second))
	if err := repository.Add(telemetry.Sample{
		ID: "s1", AssetID: "R07", Kind: telemetry.KindTemperature, Value: 40,
		Sequence: 1, ObservedAt: testBase,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	prediction := NewReplayer().Predict(context.Background(), snapshot)
	if findFinding(prediction.Risks, "telemetry-stale") == nil {
		t.Fatalf("expected telemetry-stale risk, got %+v", prediction.Risks)
	}
}

func TestEmptyTelemetryWindowRisk(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	_, _, _, captor := harness(t, topo, routeSnap, testBase)
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	prediction := NewReplayer().Predict(context.Background(), snapshot)
	if findFinding(prediction.Risks, "telemetry-window-empty") == nil {
		t.Fatalf("expected empty-window risk, got %+v", prediction.Risks)
	}
}

func TestLastDrainTimeoutRiskShapesDrainStage(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	_, _, recorder, captor := harness(t, topo, routeSnap, testBase.Add(time.Second))
	recorder.Record(switchover.Snapshot{
		ID: "switch-past", RouteID: "R-S3", State: switchover.StateProtected, UpdatedAt: testBase,
		Events: []switchover.Event{
			{Stage: "primary-drain", Message: "draining failed primary path", At: testBase},
			{Stage: "primary-drain", Message: "protection path carries traffic after primary drain timeout", At: testBase},
			{Stage: "complete", Message: "protection path carries traffic after primary drain timeout", At: testBase},
		},
	})
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LastEvent == nil || snapshot.LastEvent.ID != "switch-past" {
		t.Fatalf("expected last switchover in snapshot, got %+v", snapshot.LastEvent)
	}
	prediction := NewReplayer().Predict(context.Background(), snapshot)
	if findFinding(prediction.Risks, "last-drain-timeout") == nil {
		t.Fatalf("expected last-drain-timeout risk, got %+v", prediction.Risks)
	}
	if prediction.DrainElapsedNS != DefaultThresholds().DrainBudget.Nanoseconds() {
		t.Fatalf("expected drain elapsed at budget, got %d", prediction.DrainElapsedNS)
	}
	var drain *StageRecord
	for index := range prediction.Stages {
		if prediction.Stages[index].Name == "primary-drain" {
			drain = &prediction.Stages[index]
		}
	}
	if drain == nil || drain.Outcome != "timeout" {
		t.Fatalf("expected timeout drain stage, got %+v", prediction.Stages)
	}
}

func TestLastFailedSwitchoverRisk(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	_, _, recorder, captor := harness(t, topo, routeSnap, testBase.Add(time.Second))
	recorder.Record(switchover.Snapshot{
		ID: "switch-bad", RouteID: "R-S3", State: switchover.StateFailed, UpdatedAt: testBase,
		Events: []switchover.Event{{Stage: "protection-activate", Message: "boom", At: testBase}},
	})
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	prediction := NewReplayer().Predict(context.Background(), snapshot)
	if findFinding(prediction.Risks, "last-switchover-failed") == nil {
		t.Fatalf("expected last-switchover-failed risk, got %+v", prediction.Risks)
	}
}

func TestSameSnapshotIsComparableAcrossRuns(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	registry, repository, recorder, _ := harness(t, topo, routeSnap, testBase.Add(time.Second))
	clock := new(advancingClock)
	service := NewService(
		NewCaptor(registry, repository, recorder, DefaultThresholds(), clock.Now),
		NewReplayer(), platform.UUIDGenerator{}, clock.Now,
	)
	first, err := service.Run(context.Background(), fakeRoute{snap: routeSnap}, "first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Replay(context.Background(), first.ID, "second")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatalf("replays must get distinct report IDs, both %s", first.ID)
	}
	if first.SnapshotFingerprint != second.SnapshotFingerprint {
		t.Fatalf("replay snapshot fingerprints differ:\n%s\n%s", first.SnapshotFingerprint, second.SnapshotFingerprint)
	}
	if first.Fingerprint != second.Fingerprint {
		t.Fatalf("replay prediction fingerprints differ:\n%s\n%s", first.Fingerprint, second.Fingerprint)
	}
	if first.Prediction.Outcome != second.Prediction.Outcome {
		t.Fatalf("prediction outcomes differ: %s vs %s", first.Prediction.Outcome, second.Prediction.Outcome)
	}
}

func TestPredictionIsDeterministic(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	_, _, _, captor := harness(t, topo, routeSnap, testBase)
	snapshot, err := captor.Capture(fakeRoute{snap: routeSnap})
	if err != nil {
		t.Fatal(err)
	}
	first := NewReplayer().Predict(context.Background(), snapshot)
	second := NewReplayer().Predict(context.Background(), snapshot)
	firstFP, _ := fingerprint(first)
	secondFP, _ := fingerprint(second)
	if firstFP != secondFP {
		t.Fatalf("identical snapshots produced different predictions: %s vs %s", firstFP, secondFP)
	}
}

func TestReplayUnknownDrillErrors(t *testing.T) {
	service := NewService(nil, NewReplayer(), platform.UUIDGenerator{}, func() time.Time { return testBase })
	if _, err := service.Replay(context.Background(), "missing", "x"); err == nil {
		t.Fatal("expected error replaying unknown drill")
	}
}

func TestSnapshotValidation(t *testing.T) {
	topo, routeSnap := primarySnapshot()
	routeSnap.SpanID = "OTHER"
	_, _, _, captor := harness(t, topo, routeSnap, testBase)
	if _, err := captor.Capture(fakeRoute{snap: routeSnap}); err == nil {
		t.Fatal("expected validation error for route/topology span mismatch")
	}
}

type advancingClock struct{ t time.Time }

func (c *advancingClock) Now() time.Time {
	c.t = c.t.Add(time.Second)
	if c.t.IsZero() {
		c.t = testBase
	}
	return c.t
}

func containsCode(items []Finding, code string) bool { return findFinding(items, code) != nil }

func findFinding(items []Finding, code string) *Finding {
	for index := range items {
		if items[index].Code == code {
			return &items[index]
		}
	}
	return nil
}
