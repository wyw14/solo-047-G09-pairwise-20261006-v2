package telemetry

import "sort"

// Assets returns the tracked asset IDs in deterministic order. Used by the
// drill snapshot capture, which is read-only and must not enumerate maps.
func (r *Repository) Assets() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	assets := make([]string, 0, len(r.samples))
	for assetID := range r.samples {
		assets = append(assets, assetID)
	}
	sort.Strings(assets)
	return assets
}
