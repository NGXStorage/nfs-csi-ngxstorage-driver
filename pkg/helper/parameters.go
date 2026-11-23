package helper

import (
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// flashtierLimitPattern is the NGX flash-tier limit value grammar shared with
// the backend's blocksize field.
var flashtierLimitPattern = regexp.MustCompile(`^((1|2|4|8|16|32|64|128)k|512|0)$`)

// NormalizeMutableParameters validates a ControllerModifyVolume mutable
// parameter map, maps bool-valued fields to real booleans, and returns the
// canonical NGX PATCH payload. Only whitelisted fields are accepted; anything
// else is rejected with InvalidArgument.
func NormalizeMutableParameters(input map[string]string) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(input))
	for rawKey, value := range input {
		key := lowerTrim(rawKey)
		if key == "" {
			return nil, status.Error(codes.InvalidArgument, "mutable parameter key is required")
		}
		switch key {
		case "compression", "deduplication", "dram_cache", "flash_cache":
			switch strings.ToLower(strings.TrimSpace(value)) {
			case "true", "on", "1":
				out[key] = true
			case "false", "off", "0", "":
				out[key] = false
			default:
				return nil, status.Errorf(codes.InvalidArgument, "mutable parameter %q must be a boolean", key)
			}
		case "permissions":
			if !permissionsPattern.MatchString(value) {
				return nil, status.Errorf(codes.InvalidArgument, "mutable parameter %q must be a 9-character 0/1 string", key)
			}
			out[key] = value
		case "flashtier_limit":
			if !flashtierLimitPattern.MatchString(value) {
				return nil, status.Errorf(codes.InvalidArgument, "mutable parameter %q has an unsupported value %q", key, value)
			}
			out[key] = value
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported mutable parameter %q", key)
		}
	}
	return out, nil
}

func lowerTrim(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

var permissionsPattern = regexp.MustCompile(`^[01]{9}$`)

// DefaultBlockSize is the NGX share block size used when a StorageClass does
// not specify one. NGX requires this field to be non-empty on share creation.
const DefaultBlockSize = "128k"

// DefaultCreateParameters are the NGX share values used when a StorageClass
// omits optional create-time fields. The map includes fields that are mutable
// later and fields that are not: it is strictly a CreateVolume policy, not a
// ControllerModifyVolume or VolumeAttributesClass policy.
//
// ControllerModifyVolume deliberately does not apply defaults. A mutable
// update must contain exactly the attributes requested by the caller.
var DefaultCreateParameters = map[string]string{
	"blocksize":       DefaultBlockSize,
	"compression":     "off",
	"deduplication":   "off",
	"flashtier_limit": "0",
	"flash_cache":     "on",
	"dram_cache":      "on",
	"permissions":     "111111111",
}
