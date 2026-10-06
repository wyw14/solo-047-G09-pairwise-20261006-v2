package drill

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
)

// Service runs drills and keeps their reports. It has no access to the live
// mutation paths: it only holds a read-only Captor and the deterministic
// Replayer, so a drill cannot change routing, alarms or repair permits.
type Service struct {
	mu       sync.RWMutex
	captor   *Captor
	replayer *Replayer
	ids      platform.IDGenerator
	now      func() time.Time
	reports  map[string]Report
	order    []string
}

func NewService(captor *Captor, replayer *Replayer, ids platform.IDGenerator, now func() time.Time) *Service {
	return &Service{
		captor: captor, replayer: replayer, ids: ids, now: now,
		reports: make(map[string]Report),
	}
}

// Run captures live state into an immutable snapshot and predicts the failover
// inside an isolated replay. Production state is only read.
func (s *Service) Run(ctx context.Context, target routeSource, reason string) (Report, error) {
	snapshot, err := s.captor.Capture(target)
	if err != nil {
		return Report{}, err
	}
	return s.runWithSnapshot(ctx, snapshot, reason)
}

// Replay reruns a prediction against a previously captured snapshot without
// touching live state, so the same snapshot can be exercised repeatedly and the
// results compared by fingerprint.
func (s *Service) Replay(ctx context.Context, reportID, reason string) (Report, error) {
	s.mu.RLock()
	previous, exists := s.reports[reportID]
	s.mu.RUnlock()
	if !exists {
		return Report{}, fmt.Errorf("drill %s not found", reportID)
	}
	return s.runWithSnapshot(ctx, previous.Snapshot, reason)
}

func (s *Service) runWithSnapshot(ctx context.Context, snapshot Snapshot, reason string) (Report, error) {
	if err := snapshot.validate(); err != nil {
		return Report{}, err
	}
	snapshotFingerprint, err := fingerprint(snapshot)
	if err != nil {
		return Report{}, fmt.Errorf("fingerprint snapshot: %w", err)
	}
	prediction := s.replayer.Predict(ctx, snapshot)
	predictionFingerprint, err := fingerprint(prediction)
	if err != nil {
		return Report{}, fmt.Errorf("fingerprint prediction: %w", err)
	}
	report := Report{
		ID: s.ids.New("drill"), RouteID: snapshot.Route.ID, Reason: reason,
		StartedAt: s.now().UTC(), Snapshot: snapshot, Prediction: prediction,
		SnapshotFingerprint: snapshotFingerprint, Fingerprint: predictionFingerprint,
	}
	s.store(report)
	return report, nil
}

func (s *Service) store(report Report) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reports[report.ID] = report
	s.order = append(s.order, report.ID)
}

// Get returns a stored drill report.
func (s *Service) Get(id string) (Report, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	report, exists := s.reports[id]
	return report, exists
}

// List returns all drill reports, oldest first.
func (s *Service) List() []Report {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reports := make([]Report, 0, len(s.order))
	for _, id := range s.order {
		reports = append(reports, s.reports[id])
	}
	return reports
}
