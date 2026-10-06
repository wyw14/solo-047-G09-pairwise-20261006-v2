package route

import (
	"context"
	"fmt"
	"time"
)

type Drainer struct {
	delay     time.Duration
	simulated bool
}

func NewDrainer(delay time.Duration) *Drainer {
	return &Drainer{delay: delay}
}

// NewSimulatedDrainer models a drain that takes delay without sleeping. It is
// intended for isolated switchover drills: when the simulated duration reaches
// the caller's context deadline it returns the same interruption error as a
// real timed-out drain, so the failover state machine exercises its real
// deadline branch deterministically.
func NewSimulatedDrainer(delay time.Duration) *Drainer {
	if delay < 0 {
		delay = 0
	}
	return &Drainer{delay: delay, simulated: true}
}

func (d *Drainer) Drain(ctx context.Context, route *Route, at time.Time) (Snapshot, error) {
	before := route.Snapshot()
	route.update(StateDraining, before.ActivePath, before.TrafficBPS, at)
	if d.simulated {
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
			if remaining := time.Until(deadline); d.delay > remaining {
				return route.Snapshot(), fmt.Errorf("route drain interrupted: %w", context.DeadlineExceeded)
			}
		}
		return route.update(StateDraining, before.ActivePath, 0, at.Add(d.delay)), nil
	}
	if d.delay > 0 {
		timer := time.NewTimer(d.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return route.Snapshot(), fmt.Errorf("route drain interrupted: %w", ctx.Err())
		case <-timer.C:
		}
	}
	return route.update(StateDraining, before.ActivePath, 0, at.Add(d.delay)), nil
}

func (d *Drainer) Delay() time.Duration {
	return d.delay
}
