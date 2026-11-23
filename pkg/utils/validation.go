package utils

import (
	"context"
	"errors"
	"fmt"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BackendStatusError translates a typed NGX API failure into the CSI status
// expected by the external provisioner. It never retries or changes storage.
func BackendStatusError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if errors.Is(err, ngxstorage.ErrSnapshotInUse) {
		return status.Errorf(codes.FailedPrecondition, "%s: snapshot is in use", operation)
	}
	if grpcStatus, ok := status.FromError(err); ok && grpcStatus.Code() != codes.Unknown {
		return err
	}
	code := codes.Internal
	switch ClassifyError(err) {
	case ErrorNotFound:
		code = codes.NotFound
	case ErrorAlreadyExists:
		code = codes.AlreadyExists
	case ErrorUnavailable:
		code = codes.Unavailable
	case ErrorInvalidArgument:
		code = codes.InvalidArgument
	}
	return status.Error(code, fmt.Sprintf("%s: %v", operation, err))
}

// ValidateVolumeCapability accepts only mount capabilities and access modes
// advertised by this NFS driver; block-device capabilities are rejected.
func ValidateVolumeCapability(capability *csi.VolumeCapability, allowed []csi.VolumeCapability_AccessMode_Mode) error {
	if capability == nil {
		return status.Error(codes.InvalidArgument, "volume capability is required")
	}
	if capability.GetMount() == nil || capability.GetBlock() != nil {
		return status.Error(codes.InvalidArgument, "NFS requires a mount volume capability")
	}
	mode := capability.GetAccessMode().GetMode()
	if capability.GetAccessMode() == nil || mode == csi.VolumeCapability_AccessMode_UNKNOWN {
		return status.Error(codes.InvalidArgument, "volume capability access mode is required")
	}
	for _, candidate := range allowed {
		if candidate == mode {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "unsupported volume capability access mode: %s", mode.String())
}
