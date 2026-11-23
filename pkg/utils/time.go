package utils

import (
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// BackendCreatedTimestamp converts the NGX canonical created epoch string to
// the CSI timestamp representation. The backend response field is `created`.
func BackendCreatedTimestamp(value string) *timestamppb.Timestamp {
	epoch, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || epoch < 0 {
		return nil
	}
	if epoch > 1_000_000_000_000 {
		return timestamppb.New(time.UnixMilli(epoch))
	}
	return timestamppb.New(time.Unix(epoch, 0))
}
