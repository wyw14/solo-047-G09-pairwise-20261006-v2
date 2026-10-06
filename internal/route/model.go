package route

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

type State string

const (
	StatePrimary          State = "primary"
	StateDraining         State = "draining"
	StateProtectionActive State = "protection-active"
	StateRestoring        State = "restoring"
)

type Snapshot struct {
	ID         string        `json:"id"`
	SpanID     string        `json:"span_id"`
	State      State         `json:"state"`
	ActivePath topology.Path `json:"active_path"`
	TrafficBPS int64         `json:"traffic_bps"`
	UpdatedAt  time.Time     `json:"updated_at"`
}

type Route struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

func New(id, spanID string, at time.Time) (*Route, error) {
	if id == "" || spanID == "" {
		return nil, fmt.Errorf("route identity is required")
	}
	return &Route{snapshot: Snapshot{
		ID: id, SpanID: spanID, State: StatePrimary, ActivePath: topology.PathPrimary,
		TrafficBPS: 1_000_000, UpdatedAt: at.UTC(),
	}}, nil
}

// NewFromSnapshot rebuilds a route from a captured snapshot. Drills use it to
// replay traffic state inside an isolated copy; live routes are only created
// through New and mutated through the drain/activate services.
func NewFromSnapshot(snapshot Snapshot) (*Route, error) {
	if snapshot.ID == "" || snapshot.SpanID == "" {
		return nil, fmt.Errorf("route identity is required")
	}
	if snapshot.ActivePath != topology.PathPrimary && snapshot.ActivePath != topology.PathProtection {
		return nil, fmt.Errorf("route active path is invalid")
	}
	snapshot.UpdatedAt = snapshot.UpdatedAt.UTC()
	return &Route{snapshot: snapshot}, nil
}

func (r *Route) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshot
}

func (r *Route) update(state State, path topology.Path, traffic int64, at time.Time) Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshot.State = state
	r.snapshot.ActivePath = path
	r.snapshot.TrafficBPS = traffic
	r.snapshot.UpdatedAt = at.UTC()
	return r.snapshot
}
