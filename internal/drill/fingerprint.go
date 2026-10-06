package drill

import (
	"crypto/sha256"
	"encoding/hex"
	encodingJSON "encoding/json"
)

// canonicalSnapshot is the content that defines snapshot identity. CapturedAt
// is deliberately excluded so two captures of identical live state compare
// equal even when taken moments apart.
type canonicalSnapshot struct {
	Topology  any              `json:"topology"`
	Route     any              `json:"route"`
	Telemetry []AssetTelemetry `json:"telemetry"`
	LastEvent *LastSwitchEvent `json:"last_switch_event,omitempty"`
}

type canonicalParameters struct {
	DrainTimeout int64 `json:"drain_timeout_nanos"`
	DrainDelay   int64 `json:"drain_delay_nanos"`
}

func digest(value any) string {
	data, err := encodingJSON.Marshal(value)
	if err != nil {
		// All inputs are plain JSON-able domain values.
		panic(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func fingerprint(snap Snapshot) string {
	return digest(canonicalSnapshot{
		Topology: snap.Topology, Route: snap.Route, Telemetry: snap.Telemetry, LastEvent: snap.LastEvent,
	})
}

// runID derives a stable identity from the snapshot and the parameters so the
// same snapshot replayed with the same parameters always addresses one run.
func runID(snap Snapshot, params Parameters) string {
	combined := struct {
		Snapshot   string              `json:"snapshot_fingerprint"`
		Parameters canonicalParameters `json:"parameters"`
	}{
		Snapshot: snap.Fingerprint(),
		Parameters: canonicalParameters{
			DrainTimeout: int64(params.DrainTimeout), DrainDelay: int64(params.DrainDelay),
		},
	}
	return "drill-" + digest(combined)[:16]
}
