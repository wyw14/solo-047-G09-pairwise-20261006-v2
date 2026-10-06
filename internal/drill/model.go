// Package drill provides switchover drills: read-only what-if replays of a
// captured topology generation, route state, telemetry window and the most
// recent switchover event.
//
// A drill never mutates production state. Capture copies plain values out of
// the live registries, and prediction rebuilds fresh in-memory state machines
// from those values, so the live path, alarm admission and repair permits are
// untouched. The same Snapshot can be replayed repeatedly and yields a
// byte-comparable prediction fingerprint.
package drill

import (
	"errors"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

// Thresholds binds the prediction rules to the operational limits used by the
// live services, so a drill evaluates risk against the same numbers.
type Thresholds struct {
	RepeaterTripC   float64       `json:"repeater_trip_c"`
	TelemetryMaxAge time.Duration `json:"telemetry_max_age"`
	DrainBudget     time.Duration `json:"drain_budget"`
	NominalDrain    time.Duration `json:"nominal_drain"`
}

// DefaultThresholds mirrors the limits configured in cmd/cableguard: the 85C
// repeater trip point, the 10s telemetry freshness budget, the 25ms drain
// timeout and the 4ms nominal drain.
func DefaultThresholds() Thresholds {
	return Thresholds{
		RepeaterTripC: 85, TelemetryMaxAge: 10 * time.Second,
		DrainBudget: 25 * time.Millisecond, NominalDrain: 4 * time.Millisecond,
	}
}

// Snapshot is an immutable, value-only capture of the state a drill replays.
// It holds no pointers to live state machines.
type Snapshot struct {
	Route      route.Snapshot                `json:"route"`
	Topology   topology.Snapshot             `json:"topology"`
	Telemetry  map[string][]telemetry.Sample `json:"telemetry"`
	LastEvent  *switchover.Snapshot          `json:"last_switchover,omitempty"`
	Thresholds Thresholds                    `json:"thresholds"`
	CapturedAt time.Time                     `json:"captured_at"`
}

func (s Snapshot) validate() error {
	if s.Route.ID == "" {
		return errors.New("drill snapshot requires a route")
	}
	if s.Route.SpanID == "" || s.Topology.SpanID == "" {
		return errors.New("drill snapshot requires a span")
	}
	if s.Route.SpanID != s.Topology.SpanID {
		return errors.New("drill snapshot route and topology span differ")
	}
	if err := s.Topology.Validate(); err != nil {
		return err
	}
	if s.Thresholds.RepeaterTripC <= 0 || s.Thresholds.TelemetryMaxAge <= 0 ||
		s.Thresholds.DrainBudget <= 0 || s.Thresholds.NominalDrain <= 0 {
		return errors.New("drill thresholds must be positive")
	}
	return nil
}

// Severity classifies a predicted finding.
type Severity string

const (
	SeverityBlocker Severity = "blocker"
	SeverityHigh    Severity = "high"
	SeverityMedium  Severity = "medium"
	SeverityLow     Severity = "low"
)

// Finding is a single predicted blocker or risk.
type Finding struct {
	Code     string         `json:"code"`
	Severity Severity       `json:"severity"`
	Stage    string         `json:"stage"`
	Message  string         `json:"message"`
	AssetID  string         `json:"asset_id,omitempty"`
	Detail   map[string]any `json:"detail,omitempty"`
}

// StageRecord is one replayed switchover stage, as it unfolded in the clone.
type StageRecord struct {
	Name       string    `json:"name"`
	Outcome    string    `json:"outcome"`
	Error      string    `json:"error,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	DurationNS int64     `json:"duration_ns"`
}

// Prediction is the deterministic outcome of replaying one snapshot.
type Prediction struct {
	Outcome         string        `json:"outcome"`
	Blocking        bool          `json:"blocking"`
	Blockers        []Finding     `json:"blockers"`
	Risks           []Finding     `json:"risks"`
	Stages          []StageRecord `json:"stages"`
	FinalState      string        `json:"final_state"`
	FinalGeneration uint64        `json:"final_generation"`
	FinalActivePath string        `json:"final_active_path"`
	DrainElapsedNS  int64         `json:"drain_elapsed_ns"`
}

// Report is a stored drill run. Fingerprints exclude the run ID and wall-clock
// run time, so replaying the same snapshot with the same reason compares equal.
type Report struct {
	ID                  string     `json:"id"`
	RouteID             string     `json:"route_id"`
	Reason              string     `json:"reason"`
	StartedAt           time.Time  `json:"started_at"`
	Snapshot            Snapshot   `json:"snapshot"`
	Prediction          Prediction `json:"prediction"`
	SnapshotFingerprint string     `json:"snapshot_fingerprint"`
	Fingerprint         string     `json:"fingerprint"`
}
