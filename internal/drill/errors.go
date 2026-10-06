package drill

import "fmt"

// NotFoundError means the named live entity has no state to capture.
type NotFoundError struct {
	What string
	ID   string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s %s not found", e.What, e.ID)
}

// InvalidSnapshotError means a snapshot cannot be replayed.
type InvalidSnapshotError struct {
	Reason string
}

func (e *InvalidSnapshotError) Error() string {
	return "invalid drill snapshot: " + e.Reason
}
