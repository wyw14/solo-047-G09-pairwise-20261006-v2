package drill

import (
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

// The source interfaces expose only read methods of the live services. Capture
// therefore cannot modify the live path, alarms or permits even if the
// implementation changes.
type (
	topologySource interface {
		Snapshot(spanID string) (topology.Snapshot, error)
	}
	routeSource interface {
		Snapshot() route.Snapshot
	}
	telemetrySource interface {
		AssetIDs() []string
		History(assetID string) []telemetry.Sample
	}
	switchoverSource interface {
		Last(routeID string) (switchover.Snapshot, bool)
	}
)

// Captor copies the state needed for a drill out of the live services. Every
// copied field is a value (or a freshly allocated slice/map); the returned
// Snapshot shares nothing mutable with production.
type Captor struct {
	topologies topologySource
	telemetry  telemetrySource
	events     switchoverSource
	thresholds Thresholds
	now        func() time.Time
}

func NewCaptor(topologies topologySource, samples telemetrySource, events switchoverSource, thresholds Thresholds, now func() time.Time) *Captor {
	return &Captor{topologies: topologies, telemetry: samples, events: events, thresholds: thresholds, now: now}
}

// Capture reads the route, its topology generation, the telemetry window and
// the most recent switchover event. It performs no writes.
func (c *Captor) Capture(target routeSource) (Snapshot, error) {
	routeSnapshot := target.Snapshot()
	if routeSnapshot.ID == "" || routeSnapshot.SpanID == "" {
		return Snapshot{}, fmt.Errorf("route identity is required")
	}
	topologySnapshot, err := c.topologies.Snapshot(routeSnapshot.SpanID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("capture topology: %w", err)
	}
	samples := make(map[string][]telemetry.Sample)
	for _, assetID := range c.telemetry.AssetIDs() {
		history := c.telemetry.History(assetID)
		copied := make([]telemetry.Sample, len(history))
		copy(copied, history)
		samples[assetID] = copied
	}
	snapshot := Snapshot{
		Route:      routeSnapshot,
		Topology:   topologySnapshot,
		Telemetry:  samples,
		Thresholds: c.thresholds,
		CapturedAt: c.now().UTC(),
	}
	if last, exists := c.events.Last(routeSnapshot.ID); exists {
		event := last
		event.Events = append([]switchover.Event(nil), last.Events...)
		snapshot.LastEvent = &event
	}
	if err := snapshot.validate(); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}
