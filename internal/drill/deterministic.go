package drill

import (
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
)

// scriptedClock advances a fixed step on every read. Replay timelines are
// therefore identical on every run for a given snapshot.
type scriptedClock struct {
	current time.Time
	step    time.Duration
}

func newScriptedClock(start time.Time) *scriptedClock {
	return &scriptedClock{current: start.UTC(), step: time.Millisecond}
}

func (c *scriptedClock) Now() time.Time {
	c.current = c.current.Add(c.step)
	return c.current
}

// scriptedIDs generates predictable identifiers inside a replay so reruns of
// the same snapshot cannot diverge through random UUIDs.
type scriptedIDs struct {
	counter int
}

func (g *scriptedIDs) New(prefix string) string {
	g.counter++
	if prefix == "" {
		return fmt.Sprintf("drill-%d", g.counter)
	}
	return fmt.Sprintf("drill-%s-%d", prefix, g.counter)
}

var _ platform.IDGenerator = (*scriptedIDs)(nil)
