package drill

import (
	"context"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/topology"
)

// DefaultDrainBudget mirrors the production failover drain budget when a drill
// does not override it.
const DefaultDrainBudget = 25 * time.Millisecond

// stepClock is a deterministic clock: every Now() call advances by Step. It
// makes replay timestamps reproducible from the snapshot alone, so the same
// snapshot replays identically without reference to the host clock.
type stepClock struct {
	current time.Time
}

func newStepClock(anchor time.Time) *stepClock {
	if anchor.IsZero() {
		anchor = time.Unix(0, 0).UTC()
	}
	return &stepClock{current: anchor.UTC()}
}

func (c *stepClock) Now() time.Time {
	value := c.current
	c.current = c.current.Add(time.Millisecond)
	return value
}

// replayResult holds the cloned end-state plus the switchover snapshot.
type replayResult struct {
	outcome     switchover.Snapshot
	finalRoute  route.Snapshot
	finalTopo   topology.Snapshot
	err         error
	drainBudget time.Duration
}

// replay runs the real failover state machine on cloned topology and route
// replicas. Nothing it touches is shared with the live registries.
func replay(snap Snapshot, params Parameters) replayResult {
	// Fresh isolated registry seeded with the captured generation.
	replicaTopo := topology.NewRegistry()
	if err := replicaTopo.Seed(snap.Topology); err != nil {
		// validateSnapshot already vetted the snapshot.
		return replayResult{err: err, drainBudget: drainBudget(params)}
	}
	// Fresh isolated route rebuilt from the captured state.
	replicaRoute, err := route.NewFromSnapshot(snap.Route)
	if err != nil {
		return replayResult{err: err, drainBudget: drainBudget(params)}
	}

	clock := newStepClock(snap.Route.UpdatedAt)
	// Simulated drain: no wall-clock sleeping. When the simulated drain delay
	// exceeds the drain budget the replica returns the same interruption error
	// as a real timed-out drain, driving the state machine's real branch.
	drainer := route.NewSimulatedDrainer(params.DrainDelay)
	activator := route.NewActivator(replicaTopo)
	service := switchover.NewSessionService(drainer, activator, sequenceIDs{}, clock.Now, drainBudget(params))

	outcome, failErr := service.Failover(context.Background(), replicaRoute)
	return replayResult{
		outcome:     outcome,
		finalRoute:  replicaRoute.Snapshot(),
		finalTopo:   mustTopology(replicaTopo, snap.Route.SpanID),
		err:         failErr,
		drainBudget: drainBudget(params),
	}
}

func mustTopology(registry *topology.Registry, spanID string) topology.Snapshot {
	snapshot, err := registry.Snapshot(spanID)
	if err != nil {
		return topology.Snapshot{}
	}
	return snapshot
}

func drainBudget(params Parameters) time.Duration {
	if params.DrainTimeout > 0 {
		return params.DrainTimeout
	}
	return DefaultDrainBudget
}

// sequenceIDs mints deterministic operation IDs inside the replica so replay
// output does not depend on the host UUID generator.
type sequenceIDs struct{}

func (s sequenceIDs) New(prefix string) string {
	return "drill-replay-" + prefix + "-0001"
}
