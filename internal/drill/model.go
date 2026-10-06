// Package drill runs isolated, deterministic failover switchover rehearsals.
//
// A drill captures a read-only snapshot of the live world (topology
// generation, route state, telemetry history and the most recent switchover
// event), replays the real switchover state machine inside a cloned replica,
// and predicts blockers and risks. Drills never mutate live routes, topology,
// alarms or repair permits, and they never append to the event journal.
//
// Repeating a drill against the same snapshot with the same parameters yields
// the same run ID and an identical report, so runs are comparable.
package drill

import (
	encodingJSON "encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

// Severity classifies a predicted finding.
type Severity string

const (
	SeverityBlocker Severity = "blocker"
	SeverityRisk    Severity = "risk"
	SeverityInfo    Severity = "info"
)

// Finding is a single predicted blocker or risk located at a replay stage.
type Finding struct {
	Severity Severity `json:"severity"`
	Stage    string   `json:"stage"`
	Code     string   `json:"code"`
	Message  string   `json:"message"`
}

// BlockingPoint marks a replay stage the replay could not advance past.
type BlockingPoint struct {
	Stage   string `json:"stage"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AssetTelemetry is the captured history for one telemetry asset.
type AssetTelemetry struct {
	AssetID string             `json:"asset_id"`
	Samples []telemetry.Sample `json:"samples"`
}

// LastSwitchEvent is the most recent switchover event read from the journal.
type LastSwitchEvent struct {
	ID        string             `json:"id"`
	RouteID   string             `json:"route_id"`
	State     switchover.State   `json:"state"`
	Events    []switchover.Event `json:"events"`
	Timestamp time.Time          `json:"timestamp"`
}

// Snapshot is the immutable read-only capture a drill replays against.
type Snapshot struct {
	CapturedAt time.Time         `json:"captured_at"`
	Topology   topology.Snapshot `json:"topology"`
	Route      route.Snapshot    `json:"route"`
	Telemetry  []AssetTelemetry  `json:"telemetry"`
	LastEvent  *LastSwitchEvent  `json:"last_switch_event,omitempty"`
}

// RouteID identifies the captured route.
func (s Snapshot) RouteID() string {
	return s.Route.ID
}

// SpanID identifies the captured span.
func (s Snapshot) SpanID() string {
	return s.Route.SpanID
}

// Fingerprint hashes the snapshot content. Two captures of identical live
// state share a fingerprint regardless of capture wall-clock time.
func (s Snapshot) Fingerprint() string {
	return fingerprint(s)
}

// Parameters configure one rehearsal. They are part of the run identity so
// the same snapshot is comparable across identical parameter sets.
type Parameters struct {
	// DrainTimeout bounds the primary drain; zero means "use service default".
	DrainTimeout time.Duration `json:"drain_timeout"`
	// DrainDelay simulates how long the primary drain would take in the
	// replica. Zero drains instantly. When it exceeds DrainTimeout the real
	// failover state machine hits the deadline branch, reproducing a
	// drain-timeout cutover. It never sleeps on the live path.
	DrainDelay time.Duration `json:"drain_delay"`
}

func (p Parameters) normalized() Parameters {
	if p.DrainTimeout < 0 {
		p.DrainTimeout = 0
	}
	if p.DrainDelay < 0 {
		p.DrainDelay = 0
	}
	return p
}

// Run is the stored result of one rehearsal.
type Run struct {
	ID            string         `json:"id"`
	SnapshotID    string         `json:"snapshot_fingerprint"`
	Parameters    Parameters     `json:"parameters"`
	StartedAt     time.Time      `json:"started_at"`
	Outcome       string         `json:"outcome"`
	BlockingPoint *BlockingPoint `json:"blocking_point,omitempty"`
	Findings      []Finding      `json:"findings"`
	Replay        ReplayTrace    `json:"replay"`
}

// ReplayTrace is the deterministic record of the rehearsal.
type ReplayTrace struct {
	InitialRoute route.Snapshot     `json:"initial_route"`
	FinalRoute   route.Snapshot     `json:"final_route"`
	InitialTopo  topology.Snapshot  `json:"initial_topology"`
	FinalTopo    topology.Snapshot  `json:"final_topology"`
	Timeline     []switchover.Event `json:"timeline"`
	DrainElapsed time.Duration      `json:"drain_elapsed"`
}

// Service captures snapshots and runs drills. It holds no live pointers that
// it mutates: capture is read-only and replay uses fresh clones.
type Service struct {
	topologies *topology.Registry
	routes     map[string]*route.Route
	repository *telemetry.Repository
	journal    *store.Journal
	now        func() time.Time

	mu    sync.RWMutex
	runs  map[string]*Run
	snaps map[string]*Snapshot
}

// NewService wires the drill service against the live registries. It only ever
// reads them.
func NewService(
	topologies *topology.Registry,
	routes map[string]*route.Route,
	repository *telemetry.Repository,
	journal *store.Journal,
	now func() time.Time,
) *Service {
	return &Service{
		topologies: topologies, routes: routes, repository: repository, journal: journal,
		now: now, runs: make(map[string]*Run), snaps: make(map[string]*Snapshot),
	}
}

// Capture takes a read-only snapshot of the named route's span, the route, all
// telemetry and the latest switchover event from the journal.
func (s *Service) Capture(routeID string) (Snapshot, error) {
	target := s.routes[routeID]
	if target == nil {
		return Snapshot{}, &NotFoundError{What: "route", ID: routeID}
	}
	routeSnap := target.Snapshot()
	topoSnap, err := s.topologies.Snapshot(routeSnap.SpanID)
	if err != nil {
		return Snapshot{}, err
	}
	captured := Snapshot{
		CapturedAt: s.now().UTC(),
		Topology:   topoSnap,
		Route:      routeSnap,
		Telemetry:  s.captureTelemetry(),
		LastEvent:  s.captureLastEvent(routeID),
	}
	return captured, nil
}

func (s *Service) captureTelemetry() []AssetTelemetry {
	if s.repository == nil {
		return nil
	}
	assets := s.repository.Assets()
	result := make([]AssetTelemetry, 0, len(assets))
	for _, assetID := range assets {
		history := s.repository.History(assetID)
		result = append(result, AssetTelemetry{AssetID: assetID, Samples: history})
	}
	return result
}

func (s *Service) captureLastEvent(routeID string) *LastSwitchEvent {
	if s.journal == nil {
		return nil
	}
	records, err := s.journal.Records()
	if err != nil || len(records) == 0 {
		return nil
	}
	for index := len(records) - 1; index >= 0; index-- {
		record := records[index]
		if record.Kind != "switchover" {
			continue
		}
		var snap switchover.Snapshot
		if err := encodingJSON.Unmarshal(record.Payload, &snap); err != nil {
			continue
		}
		// Prefer the most recent event for this route; keep scanning if the
		// latest switchover belongs to a different route.
		if snap.RouteID != routeID {
			continue
		}
		return &LastSwitchEvent{
			ID: snap.ID, RouteID: snap.RouteID, State: snap.State,
			Events: append([]switchover.Event(nil), snap.Events...), Timestamp: record.Timestamp,
		}
	}
	return nil
}

// Run executes a deterministic rehearsal against a captured snapshot.
func (s *Service) Run(snap Snapshot, params Parameters) (Run, error) {
	params = params.normalized()
	if err := validateSnapshot(snap); err != nil {
		return Run{}, err
	}
	run := buildRun(snap, params, replay(snap, params))

	s.mu.Lock()
	s.runs[run.ID] = &run
	s.snaps[run.SnapshotID] = &snap
	s.mu.Unlock()
	return run, nil
}

// CaptureAndRun is a convenience used by the API.
func (s *Service) CaptureAndRun(routeID string, params Parameters) (Snapshot, Run, error) {
	snap, err := s.Capture(routeID)
	if err != nil {
		return Snapshot{}, Run{}, err
	}
	run, err := s.Run(snap, params)
	return snap, run, err
}

// GetRun returns a stored rehearsal by ID.
func (s *Service) GetRun(id string) (Run, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run := s.runs[id]
	if run == nil {
		return Run{}, false
	}
	return *run, true
}

// GetSnapshot returns the captured snapshot keyed by fingerprint.
func (s *Service) GetSnapshot(fingerprint string) (Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := s.snaps[fingerprint]
	if snap == nil {
		return Snapshot{}, false
	}
	return *snap, true
}

// Runs returns stored rehearsal IDs in deterministic order.
func (s *Service) Runs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.runs))
	for id := range s.runs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func validateSnapshot(snap Snapshot) error {
	if snap.Route.ID == "" || snap.Route.SpanID == "" {
		return &InvalidSnapshotError{Reason: "route identity is required"}
	}
	if snap.Topology.SpanID == "" || snap.Topology.Generation == 0 {
		return &InvalidSnapshotError{Reason: "topology span and generation are required"}
	}
	if snap.Topology.SpanID != snap.Route.SpanID {
		return &InvalidSnapshotError{Reason: "topology span does not match route span"}
	}
	return nil
}
