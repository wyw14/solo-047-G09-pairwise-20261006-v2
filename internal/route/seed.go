package route

import (
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

// NewFromSnapshot rebuilds a route inside an isolated drill replica from an
// exact live snapshot. It never touches the live route.
func NewFromSnapshot(snapshot Snapshot) (*Route, error) {
	if snapshot.ID == "" || snapshot.SpanID == "" {
		return nil, fmt.Errorf("route identity is required")
	}
	if snapshot.State == "" || snapshot.ActivePath != topology.PathPrimary && snapshot.ActivePath != topology.PathProtection {
		return nil, fmt.Errorf("route snapshot is invalid")
	}
	if snapshot.UpdatedAt.IsZero() {
		snapshot.UpdatedAt = time.Time{}.UTC()
	}
	return &Route{snapshot: snapshot}, nil
}
