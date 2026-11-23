// Package helper owns CSI-solution-specific backend interpretation and state.
package helper

import "strings"

// DataPath is the canonical base path used by NGX NFS exports.
const DataPath = "/exports"

// ParseBoolPayload converts StorageClass string values into the boolean shape
// expected by the NGX share API.
func ParseBoolPayload(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "on", "true", "1":
		return true
	default:
		return false
	}
}

// StaticMountRoot resolves the host-shared location used for static-PV setup.
func StaticMountRoot(configured, fallback string) string {
	if configured != "" {
		return configured
	}
	return fallback
}
