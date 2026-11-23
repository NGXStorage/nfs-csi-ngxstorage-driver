package utils

import "testing"

func TestBackendCreatedTimestamp(t *testing.T) {
	got := BackendCreatedTimestamp("1785496194")
	if got == nil || got.Seconds != 1785496194 {
		t.Fatalf("BackendCreatedTimestamp() = %v", got)
	}
	if BackendCreatedTimestamp("created-at") != nil {
		t.Fatal("invalid created value must return nil")
	}
}
