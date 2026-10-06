package drill

import (
	"strings"
	"time"

	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

// Telemetry freshness horizon: samples older than this relative to capture are
// treated as a stale picture of the protection path.
const telemetryFreshnessHorizon = 30 * time.Second

// Analytic thresholds reused from the production repeater stability policy.
var currentStability = telemetry.StabilityWindow{
	MinimumSamples: 2,
	MaximumSpread:  1.5,
	MinimumPeriod:  0,
}

// buildRun turns a deterministic replay into a comparable, findings-rich run.
func buildRun(snap Snapshot, params Parameters, result replayResult) Run {
	findings := make([]Finding, 0, 8)

	// Pre-replay structural predictors.
	findings = append(findings, structuralFindings(snap)...)
	// Telemetry-driven predictors.
	findings = append(findings, telemetryFindings(snap)...)
	// Evidence from the most recent real switchover.
	findings = append(findings, historyFindings(snap)...)
	// What the isolated replay actually did.
	findings = append(findings, replayFindings(snap, result)...)

	findings = sortFindings(findings)

	outcome := "protected"
	var blocking *BlockingPoint
	if result.err != nil || result.outcome.State == switchover.StateFailed {
		outcome = "failed"
		blocking = blockingPointFrom(result.outcome)
	} else if hasDrainInterruption(result.outcome) {
		outcome = "protected-with-drain-timeout"
	} else if anySeverity(findings, SeverityBlocker) {
		// Analytic blocker not hit in the forgiving replica (e.g. recurrence
		// of the last event's failure) is still predicted to block live.
		outcome = "blocked-predicted"
		for _, finding := range findings {
			if finding.Severity == SeverityBlocker {
				blocking = &BlockingPoint{Stage: finding.Stage, Code: finding.Code, Message: finding.Message}
				break
			}
		}
	}

	return Run{
		ID:            runID(snap, params),
		SnapshotID:    snap.Fingerprint(),
		Parameters:    params,
		StartedAt:     snap.Route.UpdatedAt.UTC(),
		Outcome:       outcome,
		BlockingPoint: blocking,
		Findings:      findings,
		Replay: ReplayTrace{
			InitialRoute: snap.Route,
			FinalRoute:   result.finalRoute,
			InitialTopo:  snap.Topology,
			FinalTopo:    result.finalTopo,
			Timeline:     append([]switchover.Event(nil), result.outcome.Events...),
			DrainElapsed: drainElapsed(result.outcome),
		},
	}
}

func structuralFindings(snap Snapshot) []Finding {
	var findings []Finding
	if snap.Route.ActivePath != snap.Topology.ActivePath {
		findings = append(findings, Finding{
			Severity: SeverityRisk,
			Stage:    "preconditions",
			Code:     "path-state-divergence",
			Message: "route active path " + string(snap.Route.ActivePath) +
				" diverges from topology active path " + string(snap.Topology.ActivePath) +
				"; cross-connect guards could blackhole traffic",
		})
	}
	if snap.Route.State == "protection-active" || snap.Route.ActivePath == topology.PathProtection {
		findings = append(findings, Finding{
			Severity: SeverityInfo,
			Stage:    "preconditions",
			Code:     "already-on-protection",
			Message:  "traffic already runs on the protection path; a new failover is a no-op cutover",
		})
	}
	if snap.Route.TrafficBPS > 0 {
		findings = append(findings, Finding{
			Severity: SeverityInfo,
			Stage:    "primary-drain",
			Code:     "traffic-at-risk",
			Message:  "carried traffic exposed during the drain window before protection carries it",
		})
	}
	return findings
}

func telemetryFindings(snap Snapshot) []Finding {
	var findings []Finding
	if len(snap.Telemetry) == 0 {
		return []Finding{{
			Severity: SeverityRisk,
			Stage:    "preconditions",
			Code:     "no-telemetry",
			Message:  "no telemetry captured; protection path health is unobservable during cutover",
		}}
	}
	for _, asset := range snap.Telemetry {
		findings = append(findings, assetTelemetryFindings(snap, asset)...)
	}
	return findings
}

func assetTelemetryFindings(snap Snapshot, asset AssetTelemetry) []Finding {
	var findings []Finding
	if len(asset.Samples) == 0 {
		return findings
	}
	latest := asset.Samples[len(asset.Samples)-1]
	if snap.CapturedAt.Sub(latest.ObservedAt) > telemetryFreshnessHorizon {
		findings = append(findings, Finding{
			Severity: SeverityRisk,
			Stage:    "preconditions",
			Code:     "stale-telemetry",
			Message:  "asset " + asset.AssetID + " latest sample predates capture by more than the freshness horizon",
		})
	}
	if latest.Generation != 0 && latest.Generation < snap.Topology.Generation {
		findings = append(findings, Finding{
			Severity: SeverityRisk,
			Stage:    "preconditions",
			Code:     "telemetry-generation-behind",
			Message: "asset " + asset.AssetID + " telemetry generation is behind topology generation " +
				"captured for the cutover",
		})
	}
	var currents []telemetry.Sample
	for _, sample := range asset.Samples {
		if sample.Kind == telemetry.KindCurrent {
			currents = append(currents, sample)
		}
	}
	if len(currents) == 0 {
		return findings
	}
	if len(currents) > 5 {
		currents = currents[len(currents)-5:]
	}
	result, err := currentStability.Evaluate(currents)
	if err == nil && !result.Stable && result.Samples >= currentStability.MinimumSamples {
		findings = append(findings, Finding{
			Severity: SeverityRisk,
			Stage:    "protection-activate",
			Code:     "unstable-repeater-current",
			Message:  "asset " + asset.AssetID + " pump current is unstable near cutover; protection energization may oscillate",
		})
	}
	return findings
}

func historyFindings(snap Snapshot) []Finding {
	if snap.LastEvent == nil {
		return nil
	}
	last := snap.LastEvent
	if last.State == switchover.StateFailed {
		stage, message := lastFailure(switchover.Snapshot{ID: last.ID, State: last.State, Events: last.Events})
		return []Finding{{
			Severity: SeverityBlocker,
			Stage:    stage,
			Code:     "previous-switchover-failed",
			Message:  "most recent switchover " + last.ID + " failed at " + stage + ": " + message,
		}}
	}
	for _, event := range last.Events {
		if strings.Contains(event.Message, "drain timeout") {
			return []Finding{{
				Severity: SeverityRisk,
				Stage:    "primary-drain",
				Code:     "previous-drain-timeout",
				Message:  "most recent switchover only cut over after the primary drain timeout",
			}}
		}
	}
	return nil
}

func replayFindings(snap Snapshot, result replayResult) []Finding {
	var findings []Finding
	if result.err != nil || result.outcome.State == switchover.StateFailed {
		stage, message := lastFailure(result.outcome)
		findings = append(findings, Finding{
			Severity: SeverityBlocker,
			Stage:    stage,
			Code:     activationErrorCode(message),
			Message:  "replay blocked at " + stage + ": " + message,
		})
		return findings
	}
	if hasDrainInterruption(result.outcome) {
		findings = append(findings, Finding{
			Severity: SeverityRisk,
			Stage:    "primary-drain",
			Code:     "drain-timeout",
			Message:  "primary drain did not settle within the configured budget; traffic cutover is forced",
		})
	}
	if result.finalTopo.Generation > snap.Topology.Generation {
		findings = append(findings, Finding{
			Severity: SeverityInfo,
			Stage:    "protection-activate",
			Code:     "topology-generation-advance",
			Message:  "cutover advances the topology generation as the cross-connect opens and switches",
		})
	}
	return findings
}

func hasDrainInterruption(snap switchover.Snapshot) bool {
	for _, event := range snap.Events {
		if event.Stage == "primary-drain" &&
			!strings.Contains(event.Message, "draining failed primary path") {
			return true
		}
	}
	return false
}

func lastFailure(snap switchover.Snapshot) (string, string) {
	stage := "protection-activate"
	message := "protection activation failed"
	for _, event := range snap.Events {
		if strings.Contains(strings.ToLower(event.Message), "error") ||
			strings.Contains(event.Message, "timeout") ||
			strings.Contains(event.Message, "interrupted") ||
			strings.Contains(event.Message, "must be open") ||
			strings.Contains(event.Message, "not found") {
			stage = event.Stage
			message = event.Message
		}
	}
	return stage, message
}

func activationErrorCode(message string) string {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "must be open"):
		return "cross-connect-guard"
	case strings.Contains(lower, "not found"):
		return "missing-topology"
	case strings.Contains(lower, "interrupted") || strings.Contains(lower, "canceled") || strings.Contains(lower, "deadline"):
		return "drain-budget-exceeded"
	default:
		return "protection-activation-failed"
	}
}

func blockingPointFrom(snap switchover.Snapshot) *BlockingPoint {
	stage, message := lastFailure(snap)
	return &BlockingPoint{Stage: stage, Code: activationErrorCode(message), Message: message}
}

func drainElapsed(snap switchover.Snapshot) time.Duration {
	var drainStart, drainEnd time.Time
	for _, event := range snap.Events {
		if event.Stage == "primary-drain" {
			if drainStart.IsZero() {
				drainStart = event.At
			}
			drainEnd = event.At
		}
		if event.Stage == "protection-activate" && !drainStart.IsZero() {
			return event.At.Sub(drainStart)
		}
	}
	if drainStart.IsZero() {
		return 0
	}
	return drainEnd.Sub(drainStart)
}

func anySeverity(findings []Finding, severity Severity) bool {
	for _, finding := range findings {
		if finding.Severity == severity {
			return true
		}
	}
	return false
}

func severityRank(severity Severity) int {
	switch severity {
	case SeverityBlocker:
		return 0
	case SeverityRisk:
		return 1
	default:
		return 2
	}
}

func sortFindings(findings []Finding) []Finding {
	// De-duplicate identical (stage, code) findings, then order deterministically.
	seen := make(map[string]bool, len(findings))
	unique := make([]Finding, 0, len(findings))
	for _, finding := range findings {
		key := finding.Stage + "\x00" + finding.Code
		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, finding)
	}
	for i := 0; i < len(unique); i++ {
		for j := i + 1; j < len(unique); j++ {
			ri, rj := severityRank(unique[i].Severity), severityRank(unique[j].Severity)
			if rj < ri ||
				(ri == rj && unique[j].Stage < unique[i].Stage) ||
				(ri == rj && unique[j].Stage == unique[i].Stage && unique[j].Code < unique[i].Code) {
				unique[i], unique[j] = unique[j], unique[i]
			}
		}
	}
	return unique
}
