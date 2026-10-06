package drill

import (
	"sync"

	"github.com/wyw14/cry-103/internal/switchover"
)

// SwitchoverRecorder keeps the most recent switchover result per route. It is a
// read-only byproduct of the live failover path: the switchover service does
// not know about it, and the recorder never feeds back into routing, alarms or
// permits. Drills read a value copy through Last.
type SwitchoverRecorder struct {
	mu     sync.RWMutex
	latest map[string]switchover.Snapshot
}

func NewSwitchoverRecorder() *SwitchoverRecorder {
	return &SwitchoverRecorder{latest: make(map[string]switchover.Snapshot)}
}

// Record stores a value copy of the operation snapshot.
func (r *SwitchoverRecorder) Record(snapshot switchover.Snapshot) {
	if snapshot.RouteID == "" {
		return
	}
	snapshot.Events = append([]switchover.Event(nil), snapshot.Events...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.latest[snapshot.RouteID] = snapshot
}

// Last returns a defensive copy of the most recent switchover for the route.
func (r *SwitchoverRecorder) Last(routeID string) (switchover.Snapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot, exists := r.latest[routeID]
	if !exists {
		return switchover.Snapshot{}, false
	}
	snapshot.Events = append([]switchover.Event(nil), snapshot.Events...)
	return snapshot, true
}
