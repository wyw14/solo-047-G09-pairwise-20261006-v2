package drill

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

// Replayer predicts a failover from an immutable snapshot. It is stateless and
// deterministic: the same Snapshot always produces the same Prediction.
type Replayer struct{}

func NewReplayer() *Replayer { return &Replayer{} }

func (r *Replayer) Predict(ctx context.Context, snap Snapshot) Prediction {
	blockers, skipReplay := r.precheck(snap)
	risks := r.risks(snap)
	prediction := Prediction{
		Blockers: ensureFindings(blockers), Risks: ensureFindings(risks), Stages: []StageRecord{},
		Blocking: len(blockers) > 0,
	}

	if skipReplay {
		prediction.Outcome = "blocked"
		prediction.FinalState = string(snap.Route.State)
		prediction.FinalGeneration = snap.Topology.Generation
		prediction.FinalActivePath = string(snap.Topology.ActivePath)
		return prediction
	}

	stages, result, err := r.replay(ctx, snap, risks)
	prediction.Stages = stages
	prediction.DrainElapsedNS = r.drainElapsed(snap, risks)
	if err != nil {
		prediction.Blocking = true
		prediction.Outcome = "failed"
		prediction.Blockers = append(prediction.Blockers, Finding{
			Code: "protection-activate-failed", Severity: SeverityBlocker, Stage: "protection-activate",
			Message: fmt.Sprintf("protection path activation would fail: %s", err.Error()),
		})
	} else {
		prediction.Outcome = "protected"
	}
	prediction.FinalState = string(result.State)
	prediction.FinalGeneration = result.Generation
	prediction.FinalActivePath = string(result.ActivePath)
	return prediction
}

// precheck returns structural conditions under which a failover cannot start
// and whether the isolated replay must be skipped.
func (r *Replayer) precheck(snap Snapshot) ([]Finding, bool) {
	var blockers []Finding
	if snap.Route.ActivePath != snap.Topology.ActivePath {
		blockers = append(blockers, Finding{
			Code: "path-divergence", Severity: SeverityBlocker, Stage: "precheck",
			Message: fmt.Sprintf("route carries %s but topology generation %d is on %s; cross-connect sequence is invalid",
				snap.Route.ActivePath, snap.Topology.Generation, snap.Topology.ActivePath),
			Detail: map[string]any{
				"route_path":    string(snap.Route.ActivePath),
				"topology_path": string(snap.Topology.ActivePath),
			},
		})
	}
	if snap.Route.ActivePath == topology.PathProtection {
		blockers = append(blockers, Finding{
			Code: "already-on-protection", Severity: SeverityBlocker, Stage: "precheck",
			Message: "route already carries traffic on the protection path; there is no primary path to fail over",
		})
	}
	return blockers, len(blockers) > 0
}

// risks evaluates telemetry and the most recent switchover event against the
// captured thresholds. It never opens incidents or changes alarm state.
func (r *Replayer) risks(snap Snapshot) []Finding {
	var risks []Finding
	var newestSample time.Time
	var samples int
	assetIDs := make([]string, 0, len(snap.Telemetry))
	for assetID := range snap.Telemetry {
		assetIDs = append(assetIDs, assetID)
	}
	sort.Strings(assetIDs)
	for _, assetID := range assetIDs {
		history := snap.Telemetry[assetID]
		samples += len(history)
		latest, ok := latestSample(history, telemetry.KindTemperature)
		if ok && latest.Value >= snap.Thresholds.RepeaterTripC {
			risks = append(risks, Finding{
				Code: "repeater-tripped", Severity: SeverityHigh, Stage: "protection-carry", AssetID: assetID,
				Message: fmt.Sprintf("asset %s temperature %.1fC is at/above the %.1fC trip point at capture",
					assetID, latest.Value, snap.Thresholds.RepeaterTripC),
				Detail: map[string]any{"temperature_c": latest.Value, "trip_c": snap.Thresholds.RepeaterTripC},
			})
		}
		for _, sample := range history {
			if sample.ObservedAt.After(newestSample) {
				newestSample = sample.ObservedAt
			}
		}
	}
	if samples == 0 {
		risks = append(risks, Finding{
			Code: "telemetry-window-empty", Severity: SeverityLow, Stage: "precheck",
			Message: "no telemetry samples were captured; protection path health cannot be judged",
		})
	} else if age := snap.CapturedAt.Sub(newestSample); age > snap.Thresholds.TelemetryMaxAge {
		risks = append(risks, Finding{
			Code: "telemetry-stale", Severity: SeverityMedium, Stage: "precheck",
			Message: fmt.Sprintf("newest telemetry sample is %s old, beyond the %s freshness budget",
				age.Round(time.Millisecond), snap.Thresholds.TelemetryMaxAge),
			Detail: map[string]any{"age_ns": int64(age), "budget_ns": int64(snap.Thresholds.TelemetryMaxAge)},
		})
	}
	risks = append(risks, r.lastEventRisks(snap)...)
	return risks
}

func (r *Replayer) lastEventRisks(snap Snapshot) []Finding {
	event := snap.LastEvent
	if event == nil {
		return nil
	}
	var risks []Finding
	timedOut := false
	for _, entry := range event.Events {
		if strings.Contains(entry.Message, "drain timeout") {
			timedOut = true
		}
	}
	switch event.State {
	case switchover.StateDrainingPrimary, switchover.StateActivating:
		risks = append(risks, Finding{
			Code: "switchover-in-flight", Severity: SeverityHigh, Stage: string(event.State),
			Message: fmt.Sprintf("the most recent switchover %s was still %s at capture", event.ID, event.State),
			Detail:  map[string]any{"switchover_id": event.ID},
		})
	case switchover.StateFailed:
		risks = append(risks, Finding{
			Code: "last-switchover-failed", Severity: SeverityHigh, Stage: "protection-activate",
			Message: fmt.Sprintf("the most recent switchover %s ended in state %s", event.ID, event.State),
			Detail:  map[string]any{"switchover_id": event.ID},
		})
	}
	if timedOut {
		risks = append(risks, Finding{
			Code: "last-drain-timeout", Severity: SeverityHigh, Stage: "primary-drain",
			Message: fmt.Sprintf("the most recent switchover %s only reached protection after the primary drain timed out", event.ID),
			Detail:  map[string]any{"switchover_id": event.ID, "budget_ns": int64(snap.Thresholds.DrainBudget)},
		})
	}
	return risks
}

type replayResult struct {
	State      switchover.State
	Generation uint64
	ActivePath topology.Path
}

// replay runs the real failover state machines against freshly built, isolated
// registries. The clone drainer has zero delay (instant, deterministic); the
// time the primary drain would consume on the live path is modeled separately
// from the captured configuration and prior event.
func (r *Replayer) replay(ctx context.Context, snap Snapshot, risks []Finding) ([]StageRecord, replayResult, error) {
	cloneTopology := topology.NewRegistry()
	if err := cloneTopology.Load(snap.Topology); err != nil {
		return nil, replayResult{State: switchover.StateFailed, Generation: snap.Topology.Generation, ActivePath: snap.Topology.ActivePath}, err
	}
	cloneRoute, err := route.NewFromSnapshot(snap.Route)
	if err != nil {
		return nil, replayResult{State: switchover.StateFailed, Generation: snap.Topology.Generation, ActivePath: snap.Topology.ActivePath}, err
	}
	clock := newScriptedClock(snap.CapturedAt)
	service := switchover.NewSessionService(
		route.NewDrainer(0), route.NewActivator(cloneTopology),
		&scriptedIDs{}, clock.Now, snap.Thresholds.DrainBudget,
	)
	operation, failErr := service.Failover(ctx, cloneRoute)
	stages := buildStages(operation, failErr, snap, risks)
	finalTopo, _ := cloneTopology.Snapshot(snap.Route.SpanID)
	return stages, replayResult{State: operation.State, Generation: finalTopo.Generation, ActivePath: finalTopo.ActivePath}, failErr
}

func buildStages(operation switchover.Snapshot, failErr error, snap Snapshot, risks []Finding) []StageRecord {
	groups := groupStages(operation.Events)
	drainTimedOut := hasRisk(risks, "last-drain-timeout")
	stages := make([]StageRecord, 0, len(groups))
	for _, group := range groups {
		record := StageRecord{
			Name: group.name, StartedAt: group.first().At.UTC(), FinishedAt: group.last().At.UTC(),
			DurationNS: group.last().At.Sub(group.first().At).Nanoseconds(), Outcome: "ok",
		}
		switch group.name {
		case "primary-drain":
			elapsed := snap.Thresholds.NominalDrain
			if drainTimedOut {
				elapsed = snap.Thresholds.DrainBudget
				record.Outcome = "timeout"
			}
			record.StartedAt = snap.CapturedAt.UTC()
			record.FinishedAt = record.StartedAt.Add(elapsed)
			record.DurationNS = elapsed.Nanoseconds()
		case "protection-activate":
			if failErr != nil {
				record.Outcome = "error"
				record.Error = failErr.Error()
			}
		}
		stages = append(stages, record)
	}
	return stages
}

func (r *Replayer) drainElapsed(snap Snapshot, risks []Finding) int64 {
	if hasRisk(risks, "last-drain-timeout") {
		return snap.Thresholds.DrainBudget.Nanoseconds()
	}
	return snap.Thresholds.NominalDrain.Nanoseconds()
}

type eventGroup struct {
	name   string
	events []switchover.Event
}

func (g eventGroup) first() switchover.Event { return g.events[0] }
func (g eventGroup) last() switchover.Event  { return g.events[len(g.events)-1] }

func groupStages(events []switchover.Event) []eventGroup {
	var groups []eventGroup
	for _, event := range events {
		if len(groups) > 0 && groups[len(groups)-1].name == event.Stage {
			groups[len(groups)-1].events = append(groups[len(groups)-1].events, event)
			continue
		}
		groups = append(groups, eventGroup{name: event.Stage, events: []switchover.Event{event}})
	}
	return groups
}

func latestSample(history []telemetry.Sample, kind telemetry.Kind) (telemetry.Sample, bool) {
	var latest telemetry.Sample
	found := false
	for _, sample := range history {
		if sample.Kind != kind {
			continue
		}
		if !found || sample.Sequence > latest.Sequence ||
			(sample.Sequence == latest.Sequence && sample.ObservedAt.After(latest.ObservedAt)) {
			latest, found = sample, true
		}
	}
	return latest, found
}

func hasRisk(risks []Finding, code string) bool {
	for _, risk := range risks {
		if risk.Code == code {
			return true
		}
	}
	return false
}

func ensureFindings(items []Finding) []Finding {
	if items == nil {
		return []Finding{}
	}
	return items
}
