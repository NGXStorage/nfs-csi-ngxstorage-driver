package helper

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
)

// BytesFromValue converts the canonical NGX API byte value to int64.
// The backend contract is bytes; no GiB conversion is performed here.
func BytesFromValue(value interface{}) (int64, error) {
	switch raw := value.(type) {
	case int:
		return int64(raw), nil
	case int64:
		return raw, nil
	case float64:
		return int64(raw), nil
	case json.Number:
		return raw.Int64()
	case string:
		return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	case nil:
		return 0, fmt.Errorf("byte value is missing")
	default:
		return 0, fmt.Errorf("unsupported byte value type %T", value)
	}
}

// ShareSoftQuotaBytes returns the canonical share capacity.soft_quota in
// bytes. Missing, malformed, or non-positive values return zero.
func ShareSoftQuotaBytes(share *ngxstorage.Share) int64 {
	if share == nil || share.Capacity.SoftQuota == nil {
		return 0
	}
	bytes := int64(*share.Capacity.SoftQuota)
	if bytes <= 0 {
		return 0
	}
	return bytes
}
