package topology

import (
	"fmt"
	"time"
)

func (r *Registry) Register(spanID string, path Path, at time.Time) (Snapshot, error) {
	snapshot := Snapshot{
		SpanID: spanID, Generation: 1, ActivePath: path, ChangedAt: at.UTC(),
		PrimaryOpen: path != PathPrimary, ProtectOpen: path != PathProtection,
	}
	if err := snapshot.Validate(); err != nil {
		return Snapshot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.spans[spanID]; exists {
		return Snapshot{}, fmt.Errorf("topology for span %s already exists", spanID)
	}
	r.spans[spanID] = snapshot
	return snapshot, nil
}

func (r *Registry) Snapshot(spanID string) (Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot, exists := r.spans[spanID]
	if !exists {
		return Snapshot{}, fmt.Errorf("topology for span %s not found", spanID)
	}
	return snapshot, nil
}

func (r *Registry) Generation(spanID string) (uint64, error) {
	snapshot, err := r.Snapshot(spanID)
	if err != nil {
		return 0, err
	}
	return snapshot.Generation, nil
}

func (r *Registry) Matches(spanID string, generation uint64) bool {
	snapshot, err := r.Snapshot(spanID)
	return err == nil && snapshot.Generation == generation
}

// Load rebuilds registry state from a previously captured snapshot. It exists
// for read-only consumers (such as switchover drills) that replay state inside
// an isolated copy; the live registry never calls it.
func (r *Registry) Load(snapshot Snapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans[snapshot.SpanID] = snapshot
	return nil
}
