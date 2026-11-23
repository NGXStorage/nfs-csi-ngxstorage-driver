package helper

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"

	nfs "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/nfs"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	zap "go.uber.org/zap"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// StaticTemporaryMountRoot is shared with the host through the node
// plugin directory; temporary base-export mounts live under it.
const StaticTemporaryMountRoot = "/var/lib/kubelet/plugins/nfs.csi.ngxstorage.com"

// PublishService owns the node-side NFS publish preparation steps: target
// conflict, endpoint/identity resolution, mount-state reconciliation, static
// child-directory preparation and the final mount. The RPC entry keeps the
// validate -> lock -> prepare -> mount -> result flow.
type PublishService struct {
	NodeState          NodeState
	Mounter            mount.Interface
	Mount              *MountService
	DataIP             string
	DataPath           string
	MountPermissions   uint64
	TemporaryMountRoot string
	Log                *zap.Logger
}

func (s *PublishService) log() *zap.Logger {
	if s.Log != nil {
		return s.Log
	}
	return zap.NewNop()
}

// NodeState is the node-local published-target bookkeeping surface.
type NodeState interface {
	PublishedTarget(volumeID string) (string, bool)
	SetPublished(volumeID, targetPath string)
}

// CheckTargetConflict rejects a second target path for a single-node
// single-writer volume; a conflicting claim is FailedPrecondition.
func (s *PublishService) CheckTargetConflict(volumeID, targetPath string, mode csi.VolumeCapability_AccessMode_Mode) error {
	if mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
		if existing, ok := s.NodeState.PublishedTarget(volumeID); ok && existing != targetPath {
			s.log().Error("NodePublishVolume(): single node single writer volume already published",
				zap.String("volume_id", volumeID),
				zap.String("target_path", targetPath),
				zap.String("existing_target_path", existing),
			)
			return status.Error(codes.FailedPrecondition, "volume is already published at a different target path")
		}
	}
	return nil
}

// ResolvePublishSource validates the controller-published endpoint and
// cross-checks volume identity (static handle or canonical CreateVolume
// context) so stale or mixed CSI context can never mount another share.
func (s *PublishService) ResolvePublishSource(volumeID, targetPath string, staticVolume bool, volumeContext, publishContext map[string]string, reqReadOnly bool) (server, exportPath, source string, readOnly bool, recursivePVName string, err error) {
	// ControllerPublishVolume returns an opaque but immutable NFS endpoint.
	// Reject incomplete or non-absolute paths before resolving volume identity.
	server = strings.TrimSpace(publishContext["server"])
	exportPath = strings.TrimSpace(publishContext["path"])
	if server == "" || exportPath == "" || !strings.HasPrefix(exportPath, "/") {
		s.log().Error("NodePublishVolume(): publish context is invalid",
			zap.String("volume_id", volumeID),
			zap.String("server", server),
			zap.String("path", exportPath),
		)
		return "", "", "", false, "", status.Error(codes.InvalidArgument, "publish context requires server and an absolute path")
	}

	readOnly = reqReadOnly
	source = server + ":" + exportPath
	recursivePVName = ""

	// Static existing shares are administrator-owned. ControllerPublishVolume
	// has already resolved the canonical export, so the node only verifies that
	// it still identifies the share encoded in the static CSI handle. Each PV
	// gets one child directory to prevent independent claims from sharing a
	// filesystem root.
	if staticVolume {
		shareName := ShareName(volumeID)
		recursivePVName = ExtractPVName(targetPath)
		if shareName == "" || recursivePVName == "" ||
			server != s.DataIP || path.Base(exportPath) != shareName {
			s.log().Error("NodePublishVolume(): static volume context is invalid",
				zap.String("volume_id", volumeID),
				zap.String("share_name", shareName),
				zap.String("pv_name", recursivePVName),
				zap.String("server", server),
				zap.String("export_path", exportPath),
				zap.String("expected_server", s.DataIP),
			)
			return "", "", "", false, "", status.Error(codes.InvalidArgument, "static volume context does not match publish context")
		}
		readOnly = readOnly || strings.EqualFold(volumeContext["readonly"], "true")
		source = server + ":" + path.Join(exportPath, recursivePVName)
		s.log().Info("NodePublishVolume(): publishing static recursive share",
			zap.String("volume_id", volumeID),
			zap.String("share_name", shareName),
			zap.String("pv_name", recursivePVName),
			zap.String("server", server),
			zap.String("export_path", exportPath),
			zap.String("source", source),
			zap.Bool("readonly", readOnly),
		)
	} else {
		// Cross-check canonical CreateVolume identity with ControllerPublish's
		// endpoint so stale or mixed CSI context cannot mount another share.
		shareID := volumeContext["share_id"]
		shareName := volumeContext["share_name"]
		poolName := volumeContext["pool_name"]
		expectedPath := path.Join(s.DataPath, poolName, shareName)
		if shareID == "" || shareName == "" || poolName == "" ||
			shareID != volumeID || server != s.DataIP || exportPath != expectedPath {
			s.log().Error("NodePublishVolume(): volume and publish contexts do not match",
				zap.String("volume_id", volumeID),
				zap.String("context_share_id", shareID),
				zap.String("share_name", shareName),
				zap.String("pool_name", poolName),
				zap.String("server", server),
				zap.String("export_path", exportPath),
				zap.String("expected_server", s.DataIP),
				zap.String("expected_path", expectedPath),
			)
			return "", "", "", false, "", status.Error(codes.InvalidArgument, "volume context does not match publish context")
		}
	}
	return server, exportPath, source, readOnly, recursivePVName, nil
}

// ReconcilePublishTarget inspects the effective mount state and recovers one
// stale target. compatible=true means the exact source/options are already
// mounted (idempotent success); conflicts fail AlreadyExists. Static recursive
// volumes skip source comparison (bind-mount semantics) and rebind below.
func (s *PublishService) ReconcilePublishTarget(ctx context.Context, volumeID, targetPath, source string, mountOptions []string, staticVolume bool) (compatible bool, err error) {
	// Compare the effective mount state before mounting. Static recursive
	// volumes skip this: their target is a bind mount whose mount-table
	// source is the base export, so a source comparison would conflict. The
	// static mount path below unbinds and rebinds instead.
	var mountState nfs.NFSMountState
	var conflictReason string
	if !staticVolume {
		mountState, conflictReason, err = nfs.InspectNFSMount(
			s.Mounter,
			targetPath,
			source,
			mountOptions,
		)
	}
	if err != nil {
		s.log().Error("NodePublishVolume(): mount target inspection failed",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.String("source", source),
			zap.Error(err),
		)
		return false, status.Errorf(codes.Internal, "inspect mount target: %v", err)
	}
	switch mountState {
	case nfs.NFSMountCompatible:
		s.log().Info("NodePublishVolume(): compatible target already mounted",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.String("source", source),
			zap.Strings("mount_options", mountOptions),
		)
		return true, nil
	case nfs.NFSMountConflict:
		s.log().Error("NodePublishVolume(): target has incompatible mount state",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.String("source", source),
			zap.Strings("mount_options", mountOptions),
			zap.String("reason", conflictReason),
		)
		return false, status.Errorf(codes.AlreadyExists, "target path has incompatible mount state: %s", conflictReason)
	case nfs.NFSMountStale:
		// Recover one stale target once. The final mount helper performs one
		// additional bounded inspection to close the inspection/mount race.
		s.log().Info("NodePublishVolume(): recovering stale mount target",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.String("reason", conflictReason),
		)
		if err := ctx.Err(); err != nil {
			s.log().Error("NodePublishVolume(): context ended before stale target recovery",
				zap.String("volume_id", volumeID),
				zap.String("target_path", targetPath),
				zap.Error(err),
			)
			return false, status.FromContextError(err).Err()
		}
		if err := s.Mounter.Unmount(targetPath); err != nil {
			s.log().Error("NodePublishVolume(): stale target unmount failed",
				zap.String("volume_id", volumeID),
				zap.String("target_path", targetPath),
				zap.Error(err),
			)
			return false, status.Errorf(codes.Internal, "unmount stale target: %v", err)
		}
		s.log().Info("NodePublishVolume(): stale mount target recovered",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
		)
	case nfs.NFSMountAbsent:
		s.log().Debug("NodePublishVolume(): target is not mounted",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
		)
	}
	return false, nil
}

// PrepareStaticSubdir creates the PV-specific child directory on a static
// share through a narrowly scoped temporary mount of the base export. On a
// read-only export the child must already exist (FailedPrecondition otherwise).
func (s *PublishService) PrepareStaticSubdir(ctx context.Context, volumeID, targetPath, server, exportPath, recursivePVName string, mountFlags []string) error {
	if err := ctx.Err(); err != nil {
		s.log().Error("NodePublishVolume(): context ended before static share preparation",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(err),
		)
		return status.FromContextError(err).Err()
	}
	temporaryRoot := StaticMountRoot(s.TemporaryMountRoot, StaticTemporaryMountRoot)
	if err := os.MkdirAll(temporaryRoot, os.FileMode(s.MountPermissions)); err != nil {
		s.log().Error("NodePublishVolume(): temporary mount root creation failed",
			zap.String("volume_id", volumeID),
			zap.String("temporary_root", temporaryRoot),
			zap.Error(err),
		)
		return status.Errorf(codes.Internal, "create static PV mount root: %v", err)
	}
	temporaryTarget, err := os.MkdirTemp(temporaryRoot, "ngx-nfs-recursive-")
	if err != nil {
		s.log().Error("NodePublishVolume(): temporary mount directory creation failed",
			zap.String("volume_id", volumeID),
			zap.String("pv_name", recursivePVName),
			zap.Error(err),
		)
		return status.Errorf(codes.Internal, "create temporary mount directory: %v", err)
	}

	baseSource := server + ":" + exportPath
	temporaryMountOptions, err := nfs.MountOptionsFromFlags(
		mountFlags,
		false,
	)
	if err != nil {
		if removeErr := os.Remove(temporaryTarget); removeErr != nil {
			s.log().Error("NodePublishVolume(): temporary directory cleanup failed",
				zap.String("volume_id", volumeID),
				zap.String("temporary_target", temporaryTarget),
				zap.Error(removeErr),
			)
		}
		s.log().Error("NodePublishVolume(): temporary mount options are invalid",
			zap.String("volume_id", volumeID),
			zap.Error(err),
		)
		return status.Error(codes.InvalidArgument, err.Error())
	}
	preparationReadOnly := false
	roOptions, optErr := nfs.MountOptionsFromFlags(
		mountFlags,
		true,
	)
	if optErr != nil {
		if removeErr := os.Remove(temporaryTarget); removeErr != nil {
			s.log().Error("NodePublishVolume(): temporary directory cleanup failed",
				zap.String("volume_id", volumeID),
				zap.String("temporary_target", temporaryTarget),
				zap.Error(removeErr),
			)
		}
		s.log().Error("NodePublishVolume(): temporary mount options are invalid",
			zap.String("volume_id", volumeID),
			zap.Error(optErr),
		)
		return status.Error(codes.InvalidArgument, optErr.Error())
	}
	prepReadOnly, prepErr := s.Mount.MountStaticBaseExport(
		ctx, baseSource, temporaryTarget, temporaryMountOptions, roOptions,
	)
	if prepErr != nil {
		if removeErr := os.Remove(temporaryTarget); removeErr != nil {
			s.log().Error("NodePublishVolume(): temporary directory cleanup failed",
				zap.String("volume_id", volumeID),
				zap.String("temporary_target", temporaryTarget),
				zap.Error(removeErr),
			)
		}
		s.log().Error("NodePublishVolume(): temporary base export mount failed",
			zap.String("volume_id", volumeID),
			zap.String("source", baseSource),
			zap.String("temporary_target", temporaryTarget),
			zap.Error(prepErr),
		)
		if errors.Is(prepErr, context.Canceled) || errors.Is(prepErr, context.DeadlineExceeded) {
			return status.FromContextError(prepErr).Err()
		}
		return status.Errorf(codes.Internal, "mount static base export: %v", prepErr)
	}
	preparationReadOnly = prepReadOnly

	recursivePath := filepath.Join(temporaryTarget, recursivePVName)
	if err := ctx.Err(); err != nil {
		cleanupErr := nfs.Unmount(s.Mounter, temporaryTarget)
		if cleanupErr != nil {
			s.log().Error("NodePublishVolume(): temporary mount cleanup failed after context ended",
				zap.String("volume_id", volumeID),
				zap.String("temporary_target", temporaryTarget),
				zap.Error(cleanupErr),
			)
		}
		s.log().Error("NodePublishVolume(): context ended before recursive PV directory creation",
			zap.String("volume_id", volumeID),
			zap.String("temporary_target", temporaryTarget),
			zap.Error(err),
		)
		return status.FromContextError(err).Err()
	}
	if preparationReadOnly {
		if _, statErr := os.Stat(recursivePath); statErr != nil {
			cleanupErr := nfs.Unmount(s.Mounter, temporaryTarget)
			if cleanupErr != nil {
				s.log().Error("NodePublishVolume(): temporary mount cleanup failed after directory check",
					zap.String("volume_id", volumeID),
					zap.String("temporary_target", temporaryTarget),
					zap.Error(cleanupErr),
				)
			}
			s.log().Error("NodePublishVolume(): recursive PV directory missing on read-only share",
				zap.String("volume_id", volumeID),
				zap.String("pv_name", recursivePVName),
				zap.String("recursive_path", recursivePath),
			)
			return status.Error(codes.FailedPrecondition,
				"PV child directory "+recursivePVName+" is missing on a read-only share; create it while the share is writable")
		}
		s.log().Info("NodePublishVolume(): recursive PV directory exists on read-only share",
			zap.String("volume_id", volumeID),
			zap.String("pv_name", recursivePVName),
		)
	} else if err := os.MkdirAll(recursivePath, os.FileMode(s.MountPermissions)); err != nil {
		cleanupErr := nfs.Unmount(s.Mounter, temporaryTarget)
		if cleanupErr != nil {
			s.log().Error("NodePublishVolume(): temporary mount cleanup failed after directory error",
				zap.String("volume_id", volumeID),
				zap.String("temporary_target", temporaryTarget),
				zap.Error(cleanupErr),
			)
		}
		s.log().Error("NodePublishVolume(): recursive PV directory creation failed",
			zap.String("volume_id", volumeID),
			zap.String("pv_name", recursivePVName),
			zap.String("recursive_path", recursivePath),
			zap.Error(err),
		)
		return status.Errorf(codes.Internal, "create static PV directory: %v", err)
	}
	s.log().Info("NodePublishVolume(): recursive PV directory is ready",
		zap.String("volume_id", volumeID),
		zap.String("pv_name", recursivePVName),
	)

	if err := nfs.Unmount(s.Mounter, temporaryTarget); err != nil {
		s.log().Error("NodePublishVolume(): temporary base export unmount failed",
			zap.String("volume_id", volumeID),
			zap.String("temporary_target", temporaryTarget),
			zap.Error(err),
		)
		return status.Errorf(codes.Internal, "unmount static base export: %v", err)
	}
	s.log().Debug("NodePublishVolume(): temporary base export unmounted",
		zap.String("volume_id", volumeID),
		zap.String("temporary_target", temporaryTarget),
	)
	if err := ctx.Err(); err != nil {
		s.log().Error("NodePublishVolume(): context ended after static share preparation",
			zap.String("volume_id", volumeID),
			zap.String("target_path", targetPath),
			zap.Error(err),
		)
		return status.FromContextError(err).Err()
	}
	return nil
}

// MountPublishTarget performs the final local mount: static recursive volumes
// bind-mount the prepared child from a temporary base-export mount; dynamic
// volumes mount the controller-published source directly.
func (s *PublishService) MountPublishTarget(ctx context.Context, volumeID, targetPath, source, server, exportPath, recursivePVName string, mountOptions []string, readOnly, staticVolume bool) error {
	// Create the target and mount the controller-published source locally.
	if staticVolume {
		// The NFS client cannot mount a share subdirectory directly (the
		// server exports only the share root and nfs-utils here predates the
		// recursive option), so publish a bind mount of the PV child
		// directory from a temporary mount of the base export.
		if err := os.MkdirAll(targetPath, os.FileMode(s.MountPermissions)); err != nil {
			s.log().Error("NodePublishVolume(): static target directory creation failed",
				zap.String("target_path", targetPath), zap.Error(err))
			return status.Errorf(codes.Internal, "create static target directory: %v", err)
		}
		if notMount, err := s.Mounter.IsLikelyNotMountPoint(targetPath); err == nil && !notMount {
			if err := s.Mounter.Unmount(targetPath); err != nil {
				s.log().Error("NodePublishVolume(): existing static target unbind failed",
					zap.String("target_path", targetPath), zap.Error(err))
				return status.Errorf(codes.Internal, "unbind static target: %v", err)
			}
		}
		temporaryRoot := StaticMountRoot(s.TemporaryMountRoot, StaticTemporaryMountRoot)
		baseMountDir := filepath.Join(temporaryRoot, "publish-"+recursivePVName)
		if err := os.MkdirAll(baseMountDir, os.FileMode(s.MountPermissions)); err != nil {
			s.log().Error("NodePublishVolume(): static publish directory creation failed",
				zap.String("base_mount_dir", baseMountDir), zap.Error(err))
			return status.Errorf(codes.Internal, "create static publish directory: %v", err)
		}
		if err := s.Mount.MountWithRetry(ctx, server+":"+exportPath, baseMountDir, mountOptions); err != nil {
			s.log().Error("NodePublishVolume(): static base export publish mount failed",
				zap.String("source", server+":"+exportPath), zap.Error(err))
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return status.FromContextError(err).Err()
			}
			return status.Errorf(codes.Internal, "mount static base export: %v", err)
		}
		bindOptions := []string{"bind"}
		if readOnly {
			bindOptions = append(bindOptions, "ro")
		}
		childPath := filepath.Join(baseMountDir, recursivePVName)
		if err := s.Mounter.Mount(childPath, targetPath, "", bindOptions); err != nil {
			nfs.Unmount(s.Mounter, baseMountDir)
			s.log().Error("NodePublishVolume(): static child bind mount failed",
				zap.String("child_path", childPath),
				zap.String("target_path", targetPath),
				zap.Error(err),
			)
			return status.Errorf(codes.Internal, "bind mount static child: %v", err)
		}
		// The bind mount is independent; drop the parent export mount. A
		// failure here leaves an extra mount behind but not a broken volume.
		if err := nfs.Unmount(s.Mounter, baseMountDir); err != nil {
			s.log().Warn("NodePublishVolume(): static base export unmount failed",
				zap.String("base_mount_dir", baseMountDir), zap.Error(err))
		}
	} else if err := s.Mount.MountWithRetry(ctx, source, targetPath, mountOptions); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.log().Error("NodePublishVolume(): context ended before local mount completed",
				zap.String("volume_id", volumeID),
				zap.String("source", source),
				zap.String("target_path", targetPath),
				zap.Error(err),
			)
			return status.FromContextError(err).Err()
		}
		if errors.Is(err, nfs.ErrIncompatibleNFSMount) {
			s.log().Error("NodePublishVolume(): target changed to incompatible mount state",
				zap.String("volume_id", volumeID),
				zap.String("source", source),
				zap.String("target_path", targetPath),
				zap.Error(err),
			)
			return status.Errorf(codes.AlreadyExists, "target path has incompatible mount state: %v", err)
		}
		s.log().Error("NodePublishVolume(): mount failed",
			zap.String("volume_id", volumeID),
			zap.String("source", source),
			zap.String("target_path", targetPath),
			zap.Error(err),
		)
		return status.Errorf(codes.Internal, "mount volume: %v", err)
	}
	return nil
}
