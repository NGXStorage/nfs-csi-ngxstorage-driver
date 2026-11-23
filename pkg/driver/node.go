package driver

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"

	helper "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/helper"
	nfs "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/nfs"
	"github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*
 * NodeStageVolume is intentionally unsupported. This NFS driver does not
 * advertise STAGE_UNSTAGE_VOLUME and mounts the controller-published NFS
 * source directly in NodePublishVolume. No staging directory, mount, or
 * backend state is created.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Logged only with safe fields; no request shape is accepted.
 *
 * @return *csi.NodeStageVolumeResponse: Always nil.
 * @return error: Unimplemented because staging is not advertised.
 */
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	d.log().Info("NodeStageVolume(): init")
	d.log().Debug("NodeStageVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("staging_target_path", req.GetStagingTargetPath()),
		zap.Reflect("volume_capability", req.GetVolumeCapability()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Reject staging explicitly so callers cannot mistake a successful no-op
	// for a prepared staging mount.
	d.log().Warn("NodeStageVolume(): staging is not supported",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("staging_target_path", req.GetStagingTargetPath()),
	)
	return nil, status.Error(codes.Unimplemented, "NodeStageVolume is not supported by this NFS driver")
}

/*
 * NodeUnstageVolume is intentionally unsupported because no CSI staging mount
 * is created. STAGE_UNSTAGE_VOLUME is not advertised and all local unmount
 * ownership remains in NodeUnpublishVolume.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Logged only with safe fields; no request shape is accepted.
 *
 * @return *csi.NodeUnstageVolumeResponse: Always nil.
 * @return error: Unimplemented because unstaging is not advertised.
 */
func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	d.log().Info("NodeUnstageVolume(): init")
	d.log().Debug("NodeUnstageVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("staging_target_path", req.GetStagingTargetPath()),
	)

	// Reject unstaging explicitly; NodeUnpublishVolume is the only local NFS
	// cleanup RPC implemented by this driver.
	d.log().Warn("NodeUnstageVolume(): unstaging is not supported",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("staging_target_path", req.GetStagingTargetPath()),
	)
	return nil, status.Error(codes.Unimplemented, "NodeUnstageVolume is not supported by this NFS driver")
}

/*
 * NodePublishVolume mounts one controller-published NGX NFS export on the
 * requested node target. Dynamic volumes require immutable publish_context
 * server/path fields plus canonical CreateVolume share_id/share_name/pool_name
 * fields. The two contexts are cross-checked before filesystem mutation.
 *
 * Node:
 *   - Reads the current mount table to distinguish same-source idempotency
 *     from a conflicting source already occupying the target.
 *   - Creates and mounts the target through pkg/nfs with deterministic NFS
 *     options from the CSI mount flags.
 *   - Performs no backend request and never changes NFS service/version or
 *     share export settings; ControllerPublishVolume owns export readiness.
 * Static existing-share handles use the same controller-published endpoint,
 * but mount a Kubernetes PV-specific child directory. The base export is
 * mounted temporarily only to create that child before the final mount.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id, target_path, publish_context,
 *             volume_context, and a supported mount capability.
 *
 * @return *csi.NodePublishVolumeResponse: Empty response after a new mount or
 *         same-source/same-target idempotent success.
 * @return error: InvalidArgument for malformed or inconsistent context,
 *         AlreadyExists for a conflicting target source, Unavailable for node
 *         mutation contention, and Internal for mount inspection/failure.
 */
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	d.log().Info("NodePublishVolume(): init")
	d.log().Debug("NodePublishVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("target_path", req.GetTargetPath()),
		zap.String("staging_target_path", req.GetStagingTargetPath()),
		zap.Bool("readonly", req.GetReadonly()),
		zap.Reflect("volume_capability", req.GetVolumeCapability()),
		zap.Reflect("volume_context", req.GetVolumeContext()),
		zap.Reflect("publish_context", req.GetPublishContext()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Stop before context validation or local filesystem mutation when the CSI
	// deadline has ended.
	if err := ctx.Err(); err != nil {
		d.log().Error("NodePublishVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil {
		d.log().Error("NodePublishVolume(): request is required")
		return nil, status.Error(codes.InvalidArgument, "node publish request is required")
	}
	if req.GetVolumeId() == "" {
		d.log().Error("NodePublishVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetTargetPath() == "" {
		d.log().Error("NodePublishVolume(): target path is required",
			zap.String("volume_id", req.GetVolumeId()),
		)
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	staticVolume := strings.HasPrefix(req.GetVolumeId(), DriverName+".")
	if req.GetVolumeContext() == nil && !staticVolume {
		d.log().Error("NodePublishVolume(): volume context is required",
			zap.String("volume_id", req.GetVolumeId()),
		)
		return nil, status.Error(codes.InvalidArgument, "volume context is required")
	}
	if req.GetPublishContext() == nil {
		d.log().Error("NodePublishVolume(): publish context is required",
			zap.String("volume_id", req.GetVolumeId()),
		)
		return nil, status.Error(codes.InvalidArgument, "publish context is required")
	}
	if err := utils.ValidateVolumeCapability(req.GetVolumeCapability(), d.AccessModes); err != nil {
		d.log().Error("NodePublishVolume(): volume capability is invalid",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Reflect("volume_capability", req.GetVolumeCapability()),
			zap.Error(err),
		)
		return nil, err
	}

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	volumeContext := req.GetVolumeContext()
	publishContext := req.GetPublishContext()
	d.log().Info("NodePublishVolume(): request accepted",
		zap.String("volume_id", volumeID),
		zap.String("target_path", targetPath),
		zap.Bool("readonly", req.GetReadonly()),
	)

	// Serialize mount-table and target-path mutation per volume. Different
	// volumes proceed in parallel; the wait is bounded by the CSI context.
	volumeLock, err := d.nodeState.LockVolume(ctx, volumeID)
	if err != nil {
		d.log().Error("NodePublishVolume(): context ended while waiting for volume lock",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(err),
		)
		return nil, status.FromContextError(err).Err()
	}
	defer volumeLock.Unlock()

	// A request may expire while waiting for the local mutation lock.
	if err := ctx.Err(); err != nil {
		d.log().Error("NodePublishVolume(): context ended while waiting for node mutation lock",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(err),
		)
		return nil, status.FromContextError(err).Err()
	}

	// Publish preparation steps: conflict -> identity -> mount-state ->
	// static child preparation -> final mount. Each step owns one responsibility.
	if err := d.publishService().CheckTargetConflict(volumeID, targetPath, req.GetVolumeCapability().GetAccessMode().GetMode()); err != nil {
		return nil, err
	}
	server, exportPath, source, readOnly, recursivePVName, err := d.publishService().ResolvePublishSource(volumeID, targetPath, staticVolume, volumeContext, publishContext, req.GetReadonly())
	if err != nil {
		return nil, err
	}

	d.log().Debug("NodePublishVolume(): canonical mount source resolved",
		zap.String("volume_id", volumeID),
		zap.String("source", source),
	)

	// Convert only explicit CSI mount flags; NFSv3 remains the local default.
	// Mount options affect this node only and never change NGX NFS settings.
	mountOptions, err := nfs.MountOptionsFromFlags(
		req.GetVolumeCapability().GetMount().GetMountFlags(),
		readOnly,
	)
	if err != nil {
		d.log().Error("NodePublishVolume(): mount options are invalid",
			zap.String("volume_id", volumeID),
			zap.Error(err),
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	d.log().Debug("NodePublishVolume(): mount options resolved",
		zap.Strings("mount_options", mountOptions),
	)

	compatible, err := d.publishService().ReconcilePublishTarget(ctx, volumeID, targetPath, source, mountOptions, staticVolume)
	if err != nil {
		return nil, err
	}
	if compatible {
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if staticVolume {
		if err := d.publishService().PrepareStaticSubdir(ctx, volumeID, targetPath, server, exportPath, recursivePVName, req.GetVolumeCapability().GetMount().GetMountFlags()); err != nil {
			return nil, err
		}
	}
	if err := d.publishService().MountPublishTarget(ctx, volumeID, targetPath, source, server, exportPath, recursivePVName, mountOptions, readOnly, staticVolume); err != nil {
		return nil, err
	}

	d.log().Info("NodePublishVolume(): mount completed",
		zap.String("volume_id", volumeID),
		zap.String("source", source),
		zap.String("target_path", targetPath),
		zap.Strings("mount_options", mountOptions),
	)
	d.nodeState.SetPublished(volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

/*
 * NodeUnpublishVolume removes one local NFS mount from the requested target.
 * Missing paths and existing paths that are not mount points are idempotent
 * successes. The operation uses the node mount table as source of truth, so it
 * remains correct after process restart.
 *
 * Node:
 *   - Performs no NGX request and never changes share export/service state.
 *   - Unmounts and cleans the target through pkg/nfs.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id and target_path.
 *
 * @return *csi.NodeUnpublishVolumeResponse: Empty response after unmount or
 *         idempotent missing/already-unmounted target handling.
 * @return error: InvalidArgument for missing fields, Unavailable for node
 *         mutation contention, and Internal for mount inspection/unmount
 *         failures.
 */
func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	d.log().Info("NodeUnpublishVolume(): init")
	d.log().Debug("NodeUnpublishVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("target_path", req.GetTargetPath()),
	)

	// Validate cancellation and required identity before local mount mutation.
	if err := ctx.Err(); err != nil {
		d.log().Error("NodeUnpublishVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("NodeUnpublishVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetTargetPath() == "" {
		d.log().Error("NodeUnpublishVolume(): target path is required",
			zap.String("volume_id", req.GetVolumeId()),
		)
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	d.log().Info("NodeUnpublishVolume(): request accepted",
		zap.String("volume_id", volumeID),
		zap.String("target_path", targetPath),
	)

	// Serialize unmount with any in-flight publish of the same volume. The
	// wait is bounded by the CSI context.
	volumeLock, err := d.nodeState.LockVolume(ctx, volumeID)
	if err != nil {
		d.log().Error("NodeUnpublishVolume(): context ended while waiting for volume lock",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(err),
		)
		return nil, status.FromContextError(err).Err()
	}
	defer volumeLock.Unlock()

	// Missing paths and existing non-mount targets satisfy CSI idempotency.
	targetStat := d.pathStat
	if targetStat == nil {
		targetStat = os.Stat
	}
	if _, statErr := targetStat(targetPath); os.IsNotExist(statErr) {
		d.log().Info("NodeUnpublishVolume(): target path is already absent",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
		)
		d.nodeState.DeletePublished(volumeID)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	} else if errors.Is(statErr, syscall.ESTALE) {
		// A deleted NFS export can leave a stale target that cannot be stat'ed.
		// It is still a mount and must be unmounted before Kubernetes can detach
		// and delete its VolumeAttachment.
		d.log().Info("NodeUnpublishVolume(): recovering stale mount target",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
		)
		if err := d.mounter.Unmount(targetPath); err != nil {
			d.log().Error("NodeUnpublishVolume(): stale target unmount failed",
				zap.String("volume_id", volumeID),
				zap.String("target_path", targetPath),
				zap.Error(err),
			)
			return nil, status.Errorf(codes.Internal, "unmount stale target: %v", err)
		}
		if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
			d.log().Error("NodeUnpublishVolume(): stale target cleanup failed",
				zap.String("volume_id", volumeID),
				zap.String("target_path", targetPath),
				zap.Error(err),
			)
			return nil, status.Errorf(codes.Internal, "remove stale target: %v", err)
		}
		d.nodeState.DeletePublished(volumeID)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	} else if statErr != nil {
		d.log().Error("NodeUnpublishVolume(): target path inspection failed",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(statErr),
		)
		return nil, status.Errorf(codes.Internal, "inspect target path: %v", statErr)
	}
	notMountPoint, mountErr := d.mounter.IsLikelyNotMountPoint(targetPath)
	if mountErr != nil {
		d.log().Error("NodeUnpublishVolume(): mount point inspection failed",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(mountErr),
		)
		return nil, status.Errorf(codes.Internal, "inspect mount point: %v", mountErr)
	}
	if notMountPoint {
		d.log().Info("NodeUnpublishVolume(): target path is already unmounted",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
		)
		d.nodeState.DeletePublished(volumeID)
		// CSI requires the SP to delete the file or directory it created at
		// the target path, including a leftover from a crashed unmount.
		if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
			d.log().Error("NodeUnpublishVolume(): target path removal failed",
				zap.String("volume_id", volumeID),
				zap.String("target_path", targetPath),
				zap.Error(err),
			)
			return nil, status.Errorf(codes.Internal, "remove target path: %v", err)
		}
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// Unmount through the reusable filesystem utility; no backend export call
	// is part of node-side cleanup.
	if err := ctx.Err(); err != nil {
		d.log().Error("NodeUnpublishVolume(): context ended before local unmount",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(err),
		)
		return nil, status.FromContextError(err).Err()
	}
	if err := nfs.Unmount(d.mounter, targetPath); err != nil {
		d.log().Error("NodeUnpublishVolume(): unmount failed",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "unmount volume: %v", err)
	}
	// CSI requires the SP to delete the file or directory it created at the
	// target path. The unmount above left an empty directory.
	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		d.log().Error("NodeUnpublishVolume(): target path removal failed",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "remove target path: %v", err)
	}

	d.log().Info("NodeUnpublishVolume(): volume unpublished",
		zap.String("volume_id", volumeID),
		zap.String("target_path", targetPath),
	)
	d.nodeState.DeletePublished(volumeID)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

/*
 * NodeGetVolumeStats returns statfs usage for one mounted NFS volume path.
 * Values are bytes and inodes from the node filesystem; no NGX request or
 * backend capacity conversion is performed.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id and volume_path. volume_path must currently
 *             be a mount point.
 *
 * @return *csi.NodeGetVolumeStatsResponse: Returns non-negative BYTES and
 *         INODES usage records.
 * @return error: InvalidArgument for missing fields, NotFound for an unmounted
 *         path, and Internal for mount/statfs failures.
 */
func (d *Driver) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	d.log().Info("NodeGetVolumeStats(): init")
	d.log().Debug("NodeGetVolumeStats(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("volume_path", req.GetVolumePath()),
	)

	// Validate identity and cancellation before reading node mount state.
	if err := ctx.Err(); err != nil {
		d.log().Error("NodeGetVolumeStats(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" || req.GetVolumePath() == "" {
		d.log().Error("NodeGetVolumeStats(): volume ID and path are required")
		return nil, status.Error(codes.InvalidArgument, "volume ID and volume path are required")
	}
	volumePath := req.GetVolumePath()

	// Require an active mount so an ordinary host directory is never reported
	// as storage capacity for the requested CSI volume.
	notMountPoint, err := d.mounter.IsLikelyNotMountPoint(volumePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			d.log().Error("NodeGetVolumeStats(): volume path does not exist",
				zap.String("volume_id", req.GetVolumeId()),
				zap.String("volume_path", volumePath),
				zap.Error(err),
			)
			return nil, status.Errorf(codes.NotFound, "volume path is not mounted: %s", volumePath)
		}
		d.log().Error("NodeGetVolumeStats(): mount point inspection failed",
			zap.String("volume_id", req.GetVolumeId()),
			zap.String("volume_path", volumePath),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "checking mount point: %v", err)
	}
	if notMountPoint {
		d.log().Error("NodeGetVolumeStats(): volume path is not mounted",
			zap.String("volume_id", req.GetVolumeId()),
			zap.String("volume_path", volumePath),
		)
		return nil, status.Errorf(codes.NotFound, "volume path is not mounted: %s", volumePath)
	}

	// Read byte and inode usage directly from the mounted NFS filesystem.
	if err := ctx.Err(); err != nil {
		d.log().Error("NodeGetVolumeStats(): context ended before statfs",
			zap.String("volume_id", req.GetVolumeId()),
			zap.String("volume_path", volumePath),
			zap.Error(err),
		)
		return nil, status.FromContextError(err).Err()
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(volumePath, &stat); err != nil {
		d.log().Error("NodeGetVolumeStats(): statfs failed",
			zap.String("volume_id", req.GetVolumeId()),
			zap.String("volume_path", volumePath),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "statfs: %v", err)
	}
	blockSize := int64(stat.Bsize)
	totalBytes := int64(stat.Blocks) * blockSize
	availableBytes := int64(stat.Bavail) * blockSize
	usedBytes := totalBytes - availableBytes
	totalInodes := int64(stat.Files)
	availableInodes := int64(stat.Ffree)
	usedInodes := totalInodes - availableInodes
	if totalBytes < 0 || availableBytes < 0 || usedBytes < 0 ||
		totalInodes < 0 || availableInodes < 0 || usedInodes < 0 {
		d.log().Error("NodeGetVolumeStats(): statfs returned negative usage",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Int64("total_bytes", totalBytes),
			zap.Int64("available_bytes", availableBytes),
			zap.Int64("total_inodes", totalInodes),
			zap.Int64("available_inodes", availableInodes),
		)
		return nil, status.Error(codes.Internal, "statfs returned invalid negative usage")
	}

	response := &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: availableBytes,
				Total:     totalBytes,
				Used:      usedBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: availableInodes,
				Total:     totalInodes,
				Used:      usedInodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}
	d.log().Info("NodeGetVolumeStats(): usage returned",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("volume_path", volumePath),
		zap.Int64("total_bytes", totalBytes),
		zap.Int64("available_bytes", availableBytes),
		zap.Int64("used_bytes", usedBytes),
		zap.Int64("total_inodes", totalInodes),
		zap.Int64("available_inodes", availableInodes),
		zap.Int64("used_inodes", usedInodes),
	)
	return response, nil
}

/*
 * NodeGetVolumeHealth reports one NGX backend health condition for a requested
 * volume. NFS volume health is derived entirely from the backend; no local
 * mount or statfs inspection is performed.
 *
 * Backend:
 *   - Reads the NGX cluster status, NFS service state, and pool capacity.
 *   - Returns one DEGRADED health entry only when the backend condition is
 *     not StorageOk.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id.
 *
 * @return *csi.NodeGetVolumeHealthResponse: Returns zero health statuses for
 *         a healthy backend and one DEGRADED entry otherwise.
 * @return error: InvalidArgument for a missing volume ID, backend status when
 *         the health check fails, and the context status when cancellation or
 *         deadline has ended.
 */
func (d *Driver) NodeGetVolumeHealth(ctx context.Context, req *csi.NodeGetVolumeHealthRequest) (*csi.NodeGetVolumeHealthResponse, error) {
	d.log().Info("NodeGetVolumeHealth(): init")
	if err := ctx.Err(); err != nil {
		d.log().Error("NodeGetVolumeHealth(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("NodeGetVolumeHealth(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	cond, err := helper.BackendHealth(ctx, d.client, d.log())
	if err != nil {
		d.log().Error("NodeGetVolumeHealth(): backend health failed",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Error(err),
		)
		return nil, utils.BackendStatusError("backend health", err)
	}
	statuses := []*csi.VolumeHealth_VolumeHealthEntry{}
	if cond != helper.StorageOK {
		statuses = append(statuses, &csi.VolumeHealth_VolumeHealthEntry{
			Status:  csi.VolumeHealthErrorType_DEGRADED,
			Reason:  string(cond),
			Message: "NGX backend reported an unfavorable health condition",
		})
	}
	d.log().Debug("NodeGetVolumeHealth(): resolved",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("condition", string(cond)),
	)
	return &csi.NodeGetVolumeHealthResponse{VolumeHealth: &csi.VolumeHealth{
		VolumeId:       req.GetVolumeId(),
		HealthStatuses: statuses,
	}}, nil
}

/*
 * NodeGetStorageHealth reports the reachability of the NGX storage backend.
 * The response always carries one backend entry; an unreachable or degraded
 * backend is reported as a degraded condition instead of failing the RPC.
 *
 * Backend:
 *   - Reads the NGX cluster status, NFS service state, and pool capacity.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Empty CSI request.
 *
 * @return *csi.NodeGetStorageHealthResponse: Returns one storage backend
 *         health entry.
 * @return error: The context status when cancellation or deadline has ended.
 */
func (d *Driver) NodeGetStorageHealth(ctx context.Context, req *csi.NodeGetStorageHealthRequest) (*csi.NodeGetStorageHealthResponse, error) {
	d.log().Info("NodeGetStorageHealth(): init")
	if err := ctx.Err(); err != nil {
		d.log().Error("NodeGetStorageHealth(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	cond, err := helper.BackendHealth(ctx, d.client, d.log())
	if err != nil {
		// The node still reports a reachable storage backend entry. The csi-sanity
		// contract only requires a non-nil response here.
		cond = helper.StorageDegraded
	}
	// The CSI enum has no explicit healthy value: UNKNOWN plus an empty
	// reason is the de-facto "no error" representation (the csi-sanity
	// contract requires a non-nil backend entry).
	status := csi.StorageHealthErrorType_UNKNOWN_STORAGE_HEALTH_ERROR_TYPE
	reason := string(cond)
	if cond == helper.StorageOK {
		reason = ""
	}
	d.log().Debug("NodeGetStorageHealth(): resolved", zap.String("condition", string(cond)))
	return &csi.NodeGetStorageHealthResponse{BackendHealth: []*csi.NodeGetStorageHealthResponse_StorageBackendHealth{{
		Status:  status,
		Reason:  reason,
		Message: "NGX NFS storage backend",
	}}}, nil
}

/*
 * NodeExpandVolume is intentionally unsupported. ControllerExpandVolume owns
 * the complete NFS expansion operation by changing the NGX share soft quota
 * and returning node_expansion_required=false. An NFS client has no block
 * device, local filesystem, or staged mount to resize.
 *
 * The node does not advertise EXPAND_VOLUME, so a conforming CO does not call
 * this RPC after ControllerExpandVolume.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Logged only with safe identifying fields; no request is
 *             accepted for node-side expansion.
 *
 * @return *csi.NodeExpandVolumeResponse: Always nil.
 * @return error: Unimplemented because NFS expansion is controller-only.
 */
func (d *Driver) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	d.log().Info("NodeExpandVolume(): init")
	d.log().Debug("NodeExpandVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("volume_path", req.GetVolumePath()),
		zap.Reflect("capacity_range", req.GetCapacityRange()),
		zap.Reflect("volume_capability", req.GetVolumeCapability()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Reject node expansion explicitly so a caller cannot mistake a successful
	// no-op for a local resize.
	d.log().Warn("NodeExpandVolume(): node expansion is not supported for NFS",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("volume_path", req.GetVolumePath()),
	)
	return nil, status.Error(codes.Unimplemented, "NodeExpandVolume is not supported by this NFS driver")
}

/*
 * NodeGetCapabilities returns only node RPC capabilities configured during
 * driver initialization. STAGE_UNSTAGE_VOLUME is intentionally absent because
 * NFS mounts directly in NodePublishVolume.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Empty CSI request.
 *
 * @return *csi.NodeGetCapabilitiesResponse: Returns configured capabilities.
 * @return error: Context cancellation/deadline status.
 */
func (d *Driver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	d.log().Info("NodeGetCapabilities(): init")
	d.log().Debug("NodeGetCapabilities(): request",
		zap.Bool("request_present", req != nil),
	)
	if err := ctx.Err(); err != nil {
		d.log().Error("NodeGetCapabilities(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	response := &csi.NodeGetCapabilitiesResponse{Capabilities: d.nsCap}
	d.log().Info("NodeGetCapabilities(): capabilities returned",
		zap.Reflect("capabilities", response.GetCapabilities()),
	)
	return response, nil
}

/*
 * NodeGetInfo returns the configured Kubernetes node identity. NGX NFS shares
 * are reachable from every configured node, so the driver does not invent
 * per-node zone or region topology segments.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Empty CSI request.
 *
 * @return *csi.NodeGetInfoResponse: Returns node_id with no topology or volume
 *         limit restriction.
 * @return error: InvalidArgument when node_id is missing or the context status
 *         when cancellation/deadline has ended.
 */
func (d *Driver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	d.log().Info("NodeGetInfo(): init")
	d.log().Debug("NodeGetInfo(): request",
		zap.Bool("request_present", req != nil),
	)

	// Validate cancellation and the configured identity before responding.
	if err := ctx.Err(); err != nil {
		d.log().Error("NodeGetInfo(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if d.NodeId == "" {
		d.log().Error("NodeGetInfo(): node ID is empty")
		return nil, status.Error(codes.InvalidArgument, "node ID is empty")
	}

	response := &csi.NodeGetInfoResponse{NodeId: d.NodeId}
	d.log().Info("NodeGetInfo(): node identity returned",
		zap.String("node_id", d.NodeId),
	)
	return response, nil
}

// ---------------------------------------------------------------------------
// Node mount retry, transient classification, and static-PV preparation
// ---------------------------------------------------------------------------
