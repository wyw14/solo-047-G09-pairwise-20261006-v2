package drill

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// fingerprint canonicalizes a value as deterministic JSON and hashes it. Two
// drills over the same snapshot compare on SnapshotFingerprint; identical
// predictions additionally compare on Fingerprint.
func fingerprint(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
