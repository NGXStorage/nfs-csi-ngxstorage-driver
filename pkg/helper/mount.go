package helper

import (
	"context"
	"os"

	nfs "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/nfs"
	utils "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/utils"

	zap "go.uber.org/zap"
	mount "k8s.io/mount-utils"
)

// MountService owns the driver-side NFS mount orchestration: the injectable
// mount seam, bounded transient retry, and the read-only fallback for static
// base-export preparation. Mount mechanics live in pkg/nfs.
type MountService struct {
	MountFn     func(ctx context.Context, source, target string, options []string) error
	Mounter     mount.Interface
	Permissions os.FileMode
	Log         *zap.Logger
}

func (m *MountService) log() *zap.Logger {
	if m.Log != nil {
		return m.Log
	}
	return zap.NewNop()
}

// mountVolume runs one NFS mount through the injectable mount function.
func (m *MountService) Mount(ctx context.Context, source, target string, options []string) error {
	if m.MountFn != nil {
		return m.MountFn(ctx, source, target, options)
	}
	return nfs.Mount(ctx, m.Mounter, source, target, options, os.FileMode(m.Permissions))
}

// mountWithRetry retries a transient NFS mount failure (server restart or
// data-plane blip). Permanent errors (bad export, permissions, incompatible
// mount state) return immediately; the retry budget is bounded and honors the
// request context, so kubelet cancellation still wins.
func (m *MountService) MountWithRetry(ctx context.Context, source, target string, options []string) error {
	var lastErr error
	for attempt := 1; attempt <= utils.ReconcileAttempts; attempt++ {
		err := m.Mount(ctx, source, target, options)
		if err == nil {
			return nil
		}
		lastErr = err
		if !nfs.TransientNFSMountError(err) {
			return err
		}
		m.log().Warn("NFS mount transient failure, retrying",
			zap.String("source", source),
			zap.String("target", target),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		if serr := utils.ReconcileBackoff(ctx, attempt); serr != nil {
			return serr
		}
	}
	return lastErr
}

// mountStaticBaseExportForPreparation mounts the base export used to prepare
// a static PV's child directory. Transient failures are retried with the
// bounded mount helper. The read-only fallback is taken only when the writable
// attempt failed with a positively identified read-only/write-denied error; a
// connectivity failure must not be misclassified as a read-only export.
func (m *MountService) MountStaticBaseExport(ctx context.Context, source, target string, rwOptions, roOptions []string) (bool, error) {
	err := m.MountWithRetry(ctx, source, target, rwOptions)
	if err == nil {
		return false, nil
	}
	if !nfs.IsReadOnlyExportMountError(err) {
		return false, err
	}
	m.log().Info("static base export rejected the writable preparation mount, retrying read-only",
		zap.String("source", source),
		zap.Error(err),
	)
	if roErr := m.MountWithRetry(ctx, source, target, roOptions); roErr != nil {
		return true, roErr
	}
	return true, nil
}
