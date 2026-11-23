package helper

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeMutableParametersAcceptsWhitelistedFields(t *testing.T) {
	input := map[string]string{
		"compression":     "true",
		"permissions":     "111111111",
		"flash_cache":     "true",
		"dram_cache":      "false",
		"deduplication":   "on",
		"flashtier_limit": "1k",
	}
	got, err := NormalizeMutableParameters(input)
	if err != nil {
		t.Fatalf("NormalizeMutableParameters() error = %v", err)
	}
	if got["compression"] != true {
		t.Fatalf("compression = %#v, want true", got["compression"])
	}
	if got["dram_cache"] != false {
		t.Fatalf("dram_cache = %#v, want false", got["dram_cache"])
	}
	if got["deduplication"] != true {
		t.Fatalf("deduplication = %#v, want true", got["deduplication"])
	}
	if got["permissions"] != "111111111" {
		t.Fatalf("permissions = %v", got["permissions"])
	}
	if got["flashtier_limit"] != "1k" {
		t.Fatalf("flashtier_limit = %v", got["flashtier_limit"])
	}
}

func TestNormalizeMutableParametersAcceptsFlashtierLimit(t *testing.T) {
	for _, good := range []string{"0", "512", "128k"} {
		t.Run(good, func(t *testing.T) {
			got, err := NormalizeMutableParameters(map[string]string{"flashtier_limit": good})
			if err != nil {
				t.Fatalf("NormalizeMutableParameters() error = %v", err)
			}
			if got["flashtier_limit"] != good {
				t.Fatalf("flashtier_limit = %v, want %q", got["flashtier_limit"], good)
			}
		})
	}
}

func TestNormalizeMutableParametersRejectsUnknownFields(t *testing.T) {
	for name, input := range map[string]map[string]string{
		"soft_quota": {"soft_quota": "2147483648"},
		"nfs_export": {"nfs_export": "true"},
		"read_only":  {"read_only": "true"},
		"enabled":    {"enabled": "true"},
		"unknown":    {"XXX_FakeKey": "XXX_FakeValue"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NormalizeMutableParameters(input)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestNormalizeMutableParametersRejectsBadFlashtierLimit(t *testing.T) {
	for _, bad := range []string{"50", "abc", "1m", "2g", "256k", "129k"} {
		t.Run(bad, func(t *testing.T) {
			_, err := NormalizeMutableParameters(map[string]string{"flashtier_limit": bad})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestNormalizeMutableParametersRejectsBadPermissions(t *testing.T) {
	for _, bad := range []string{"11111111"} {
		t.Run(bad, func(t *testing.T) {
			_, err := NormalizeMutableParameters(map[string]string{"permissions": bad})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestNormalizeMutableParametersRejectsBadBool(t *testing.T) {
	for _, key := range []string{"compression", "deduplication", "dram_cache", "flash_cache"} {
		t.Run(key, func(t *testing.T) {
			_, err := NormalizeMutableParameters(map[string]string{key: "banana"})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestNormalizeMutableParametersRejectsEmptyKey(t *testing.T) {
	for _, input := range []map[string]string{
		{"": "x"},
		{"   ": "x"},
		{"\t": "x"},
	} {
		_, err := NormalizeMutableParameters(input)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("input %v code = %s, want %s: %v", input, status.Code(err), codes.InvalidArgument, err)
		}
	}
}
