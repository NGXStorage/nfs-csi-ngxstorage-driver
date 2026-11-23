package helper

import (
	"encoding/json"
	"testing"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
)

const unitGiB = int64(1024 * 1024 * 1024)

func flexBytesPtr(value int64) *ngxstorage.FlexBytes {
	quota := ngxstorage.FlexBytes(value)
	return &quota
}

func TestBytesFromValueCanonicalTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  int64
	}{
		{name: "int", value: 1, want: 1},
		{name: "int64", value: int64(2), want: 2},
		{name: "float64", value: float64(3), want: 3},
		{name: "json number", value: json.Number("4"), want: 4},
		{name: "string", value: " 5 ", want: 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := BytesFromValue(test.value)
			if err != nil || got != test.want {
				t.Fatalf("BytesFromValue(%v) = %d, %v; want %d", test.value, got, err, test.want)
			}
		})
	}

	for _, value := range []interface{}{nil, true, "invalid", json.Number("bad")} {
		if _, err := BytesFromValue(value); err == nil {
			t.Fatalf("BytesFromValue(%v) accepted malformed value", value)
		}
	}
}

func TestShareSoftQuotaBytesUsesCanonicalSoftQuotaOnly(t *testing.T) {
	tests := []struct {
		name  string
		share *ngxstorage.Share
		want  int64
	}{
		{
			name: "soft quota only",
			share: &ngxstorage.Share{
				Capacity: ngxstorage.ShareCapacity{SoftQuota: flexBytesPtr(unitGiB)},
			},
			want: unitGiB,
		},
		{
			name:  "no quota set",
			share: &ngxstorage.Share{},
			want:  0,
		},
		{
			name:  "nil share",
			share: nil,
			want:  0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ShareSoftQuotaBytes(test.share); got != test.want {
				t.Fatalf("ShareSoftQuotaBytes() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestShareSoftQuotaBytesRequiresPositiveBytes(t *testing.T) {
	valid := &ngxstorage.Share{
		Capacity: ngxstorage.ShareCapacity{SoftQuota: flexBytesPtr(unitGiB)},
	}
	if got := ShareSoftQuotaBytes(valid); got != unitGiB {
		t.Fatalf("ShareSoftQuotaBytes() = %d, want %d", got, unitGiB)
	}

	for _, share := range []*ngxstorage.Share{
		nil,
		{},
		{Capacity: ngxstorage.ShareCapacity{SoftQuota: flexBytesPtr(-1)}},
	} {
		if got := ShareSoftQuotaBytes(share); got != 0 {
			t.Fatalf("ShareSoftQuotaBytes(%v) = %d, want 0", share, got)
		}
	}
}
