package api

import "time"

// DrillRequest starts or reruns a switchover drill. A drill is read-only: it
// never changes the live path, alarms or repair permits.
type DrillRequest struct {
	Reason string `json:"reason"`
}

// DrillComparison summarizes how two runs of the same snapshot relate.
type DrillComparison struct {
	ReportID                  string    `json:"report_id"`
	BaseReportID              string    `json:"base_report_id,omitempty"`
	SnapshotFingerprint       string    `json:"snapshot_fingerprint"`
	PredictionFingerprint     string    `json:"prediction_fingerprint"`
	BasePredictionFingerprint string    `json:"base_prediction_fingerprint,omitempty"`
	SameSnapshot              bool      `json:"same_snapshot"`
	Comparable                bool      `json:"comparable"`
	PredictionChanged         bool      `json:"prediction_changed"`
	ComparedAt                time.Time `json:"compared_at"`
}
