package topology

// Seed installs an exact historical snapshot into a fresh, isolated registry.
// It is the clone counterpart of Snapshot and never mutates the live registry.
func (r *Registry) Seed(snapshot Snapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.spans[snapshot.SpanID]; exists {
		return &AlreadySeededError{SpanID: snapshot.SpanID}
	}
	r.spans[snapshot.SpanID] = snapshot
	return nil
}

// AlreadySeededError reports that an isolated registry already holds the span.
type AlreadySeededError struct {
	SpanID string
}

func (e *AlreadySeededError) Error() string {
	return "topology for span " + e.SpanID + " already seeded in isolated registry"
}
