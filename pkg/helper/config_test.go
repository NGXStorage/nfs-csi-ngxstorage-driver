package helper

import (
	"testing"
)

func TestParseBoolPayload(t *testing.T) {
	for _, value := range []string{"on", "true", "1", " TRUE "} {
		if !ParseBoolPayload(value) {
			t.Fatalf("%q parsed false", value)
		}
	}
	for _, value := range []string{"", "off", "false", "0"} {
		if ParseBoolPayload(value) {
			t.Fatalf("%q parsed true", value)
		}
	}
}
