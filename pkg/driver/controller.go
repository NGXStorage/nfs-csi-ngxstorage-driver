// Package driver maps CSI controller requests to canonical NGX NFS share and
// snapshot operations. Backend schemas and transport live in the unified SDK;
// this package owns request validation, orchestration, and gRPC status mapping.
package driver

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	coord "github.com/NGXStorage/csi-ngxstorage-utils/coord"
	ngxnames "github.com/NGXStorage/csi-ngxstorage-utils/names"
	"github.com/container-storage-interface/spec/lib/go/csi"
	helper "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/helper"
	"github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/utils"
	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*
 * CreateVolume provisions one NGX NFS share for a CSI volume.
 * CapacityRange and the NGX capacity.soft_quota field are expressed in bytes.
 * Same-name and same-capacity requests return the existing canonical share ID;
 * same-name requests with a different quota return AlreadyExists.
 *
 * Backend:
 *   - Reads GET /api/v2/share/list for idempotency.
 *   - Reads GET /api/v2/snapshot/list for a snapshot source.
 *   - Creates through POST /api/v2/share/ with nfs_export=false.
 *   - Clones snapshots through POST /api/v2/snapshot/clone/{id}.
 *   - Reads canonical id, name, capacity.soft_quota, and snapshot volume_id.
 * ControllerPublishVolume owns export enablement. Direct share cloning is
 * unsupported and is not advertised through CLONE_VOLUME.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires name and mount volume_capabilities. CapacityRange is
 *             optional and defaults to one GiB. Parameters are NFS share
 *             settings; secrets are never logged.
 *
 * @return *csi.CreateVolumeResponse: Returns canonical volume_id,
 *         capacity_bytes, content_source, and immutable volume_context.
 * @return error: InvalidArgument for malformed requests, AlreadyExists for a
 *         same-name/different-quota share, NotFound for a missing source,
 *         ResourceExhausted for insufficient pool capacity, Unavailable for
 *         temporary storage failure, and Internal for malformed backend data.
 */
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	d.log().Info("CreateVolume(): init")
	d.log().Debug("CreateVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("name", req.GetName()),
		zap.Reflect("capacity_range", req.GetCapacityRange()),
		zap.Reflect("volume_capabilities", req.GetVolumeCapabilities()),
		zap.Reflect("parameters", req.GetParameters()),
		zap.Reflect("content_source", req.GetVolumeContentSource()),
		zap.Reflect("accessibility_requirements", req.GetAccessibilityRequirements()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Stop before validation or storage access when the CSI deadline has ended.
	if err := ctx.Err(); err != nil {
		d.log().Error("CreateVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil {
		d.log().Error("CreateVolume(): request is required")
		return nil, status.Error(codes.InvalidArgument, "create volume request is required")
	}
	if req.GetName() == "" {
		d.log().Error("CreateVolume(): volume name is required")
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}
	capacity := req.GetCapacityRange()
	if capacity != nil &&
		(capacity.GetRequiredBytes() < 0 || capacity.GetLimitBytes() < 0) {
		d.log().Error("CreateVolume(): capacity values cannot be negative",
			zap.Int64("required_bytes", capacity.GetRequiredBytes()),
			zap.Int64("limit_bytes", capacity.GetLimitBytes()),
		)
		return nil, status.Error(codes.InvalidArgument, "capacity values cannot be negative")
	}
	if capacity != nil &&
		capacity.GetRequiredBytes() > 0 &&
		capacity.GetLimitBytes() > 0 &&
		capacity.GetRequiredBytes() > capacity.GetLimitBytes() {
		d.log().Error("CreateVolume(): required capacity exceeds limit",
			zap.Int64("required_bytes", capacity.GetRequiredBytes()),
			zap.Int64("limit_bytes", capacity.GetLimitBytes()),
		)
		return nil, status.Error(codes.InvalidArgument, "required capacity exceeds capacity limit")
	}

	// Normalize the backend share name and default omitted capacity to one GiB.
	// Both CSI capacity and NGX soft_quota remain byte values.
	volumeName := ngxnames.ModifyVolName(req.GetName())
	capacityRange := req.GetCapacityRange()
	if capacityRange == nil {
		capacityRange = &csi.CapacityRange{RequiredBytes: utils.UnitGiB}
	}
	if capacityRange.GetRequiredBytes() == 0 {
		capacityRange = &csi.CapacityRange{
			RequiredBytes: utils.UnitGiB,
			LimitBytes:    capacityRange.GetLimitBytes(),
		}
	}

	// Validate every requested capability against what the driver advertises.
	// Backing shares are always created writable on the NGX appliance; read-only
	// enforcement happens at the NFS export level during ControllerPublishVolume
	// via SetShareReadOnly.
	if len(req.GetVolumeCapabilities()) == 0 {
		d.log().Error("CreateVolume(): volume capabilities are required")
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	for _, capability := range req.GetVolumeCapabilities() {
		if err := utils.ValidateVolumeCapability(capability, d.AccessModes); err != nil {
			d.log().Error("CreateVolume(): volume capability is invalid",
				zap.Reflect("volume_capability", capability),
				zap.Error(err),
			)
			return nil, err
		}
	}

	// Accept only the documented StorageClass parameter names. They map
	// directly to canonical NGX share fields; aliases are intentionally not
	// supported.
	parameters := make(map[string]string)
	for key, value := range req.GetParameters() {
		switch key {
		case "blocksize", "flash_cache", "deduplication", "dram_cache", "compression", "permissions":
			parameters[key] = strings.TrimSpace(value)
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported StorageClass parameter %q", key)
		}
	}

	// Mutable parameters are validated so CreateVolume mirrors
	// ControllerModifyVolume's field whitelist for attribute classes.
	mutable, err := helper.NormalizeMutableParameters(req.GetMutableParameters())
	if err != nil {
		d.log().Error("CreateVolume(): mutable parameters rejected",
			zap.String("volume_id", volumeName),
			zap.Error(err),
		)
		return nil, err
	}
	// CSI mutable parameters take precedence over the corresponding
	// StorageClass parameters. Complete the normal CreateVolume request with
	// documented create defaults; this does not make the fields mutable.
	for key, value := range mutable {
		parameters[key] = fmt.Sprintf("%v", value)
	}
	for key, value := range helper.DefaultCreateParameters {
		if parameters[key] == "" {
			parameters[key] = value
		}
	}

	d.log().Info(
		"CreateVolume(): validated",
		zap.String("name", volumeName),
		zap.Int64("required_bytes", capacityRange.GetRequiredBytes()),
		zap.Reflect("parameters", parameters),
		zap.Reflect("content_source", req.GetVolumeContentSource()),
	)

	// Creating a share requires the NFS service to be running, but this driver
	// never starts it or changes the administrator's NFS version settings.
	if err := helper.CheckService(ctx, d.client, d.log()); err != nil {
		d.log().Error("CreateVolume(): NFS service check failed", zap.Error(err))
		return nil, utils.BackendStatusError("check nfs service", err)
	}
	// CapacityRange.RequiredBytes maps directly to the canonical share
	// capacity.soft_quota; both are plain byte counts with no unit conversion.
	// LimitBytes is deliberately not forwarded: the Kubernetes external
	// provisioner never populates it (kubernetes-csi/external-provisioner#49
	// removed LimitBytes so drivers that round capacity up are not rejected).
	flashCache := helper.ParseBoolPayload(parameters["flash_cache"])
	dramCache := helper.ParseBoolPayload(parameters["dram_cache"])
	createRequest := ngxstorage.ShareCreateRequest{
		Name:           volumeName,
		SoftQuotaBytes: capacityRange.GetRequiredBytes(),
		BlockSize:      parameters["blocksize"],
		FlashCache:     &flashCache,
		DramCache:      &dramCache,
		Dedup:          helper.ParseBoolPayload(parameters["deduplication"]),
		Compress:       helper.ParseBoolPayload(parameters["compression"]),
		FlashTierLimit: parameters["flashtier_limit"],
	}

	// The backend ID does not exist yet, so same-name provisioning is guarded by
	// the canonical create-name key. Snapshot clones additionally hold the source
	// snapshot lifecycle key through clone completion/readback.
	snapshotSourceID := ""
	if source := req.GetVolumeContentSource(); source != nil {
		if snapshot := source.GetSnapshot(); snapshot != nil {
			snapshotSourceID = snapshot.GetSnapshotId()
		}
	}
	createLockKeys := []string{"create-volume:" + volumeName}
	if snapshotSourceID != "" {
		createLockKeys = append(createLockKeys, "snapshot:"+snapshotSourceID)
	}

	lockNoun := "locks"
	if snapshotSourceID == "" {
		lockNoun = "lock"
	}
	if d.coordination == nil {
		return nil, status.Errorf(codes.Unavailable, "%s: coordinator is unavailable", "acquire NFS create-volume "+lockNoun)
	}
	options := coord.AcquireOptions{
		Safety: coord.SafetyOrdinary, Operation: coord.OperationCreate,
	}
	var lockSet coord.LockSet
	var acquireErr error
	if snapshotSourceID == "" {
		var lock coord.Lock
		lock, acquireErr = d.coordination.Acquire(ctx, createLockKeys[0], options)
		lockSet = lock
	} else {
		lockSet, acquireErr = d.coordination.AcquireMany(ctx, createLockKeys, options)
	}
	if acquireErr != nil {
		if errors.Is(acquireErr, context.Canceled) || errors.Is(acquireErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(acquireErr).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "acquire NFS create-volume "+lockNoun, acquireErr)
	}
	var result *csi.CreateVolumeResponse
	criticalErr := lockSet.AssertOwned(ctx)
	if criticalErr != nil {
		if errors.Is(criticalErr, context.Canceled) || errors.Is(criticalErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(criticalErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS create-volume "+lockNoun+" ownership", criticalErr)
		}
		goto release
	}
	{

		// Read canonical share records from the backend so idempotency survives
		// controller restarts and concurrent replicas. The list also refreshes the
		// process-local volume hash cache.
		shareDetailList, err := d.client.Shares().List(ctx)
		if err != nil {
			d.log().Error("CreateVolume(): share list failed", zap.Error(err))
			criticalErr = utils.BackendStatusError("list shares", err)
			goto release
		}
		d.volumeHashes.Sync(shareDetailList)

		// Resolve idempotency before checking the content source. A source snapshot
		// may legitimately be deleted after the first successful CreateVolume.
		// The volume hash maps were synced from the list above, so a tracked name
		// resolves the canonical record by id while the name fallback still catches
		// malformed records that never enter the maps.
		trackedID, tracked := d.volumeHashes.IDByName(volumeName)
		for i := range shareDetailList {
			share := shareDetailList[i]
			if tracked {
				if share.ID != trackedID {
					continue
				}
			} else if share.Name != volumeName {
				continue
			}
			shareID := share.ID
			if shareID == "" {
				d.log().Error("CreateVolume(): existing share response is missing id",
					zap.String("name", volumeName),
				)
				criticalErr = status.Error(codes.Internal, "existing share response missing id")
				goto release
			}
			quota := helper.ShareSoftQuotaBytes(&share)
			if quota == 0 {
				d.log().Error("CreateVolume(): existing share has invalid soft quota",
					zap.String("volume_id", shareID),
				)
				criticalErr = status.Errorf(codes.Internal, "existing share %s has invalid soft_quota", shareID)
				goto release
			}
			if quota != capacityRange.RequiredBytes {
				d.log().Error("CreateVolume(): existing share capacity differs",
					zap.String("volume_id", shareID),
					zap.Int64("existing_bytes", quota),
					zap.Int64("required_bytes", capacityRange.RequiredBytes),
				)
				criticalErr = status.Error(codes.AlreadyExists, "volume already exists with a different capacity")
				goto release
			}
			if permissions := parameters["permissions"]; permissions != "" {
				if err := lockSet.AssertOwned(ctx); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						criticalErr = status.FromContextError(err).Err()
					} else {
						criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify NFS create-volume ownership before setting permissions", err)
					}
					goto release
				}
				if err := d.reconciler().ModifyShareOnOwner(ctx, shareID, map[string]interface{}{"permissions": permissions}); err != nil {
					criticalErr = utils.BackendStatusError("set share permissions", err)
					goto release
				}
			}
			d.log().Info("CreateVolume(): existing idempotent volume returned",
				zap.String("volume_id", shareID),
				zap.String("name", volumeName),
				zap.Int64("capacity_bytes", capacityRange.RequiredBytes),
			)
			result = &csi.CreateVolumeResponse{Volume: &csi.Volume{
				VolumeId: shareID, CapacityBytes: capacityRange.RequiredBytes,
				ContentSource: req.GetVolumeContentSource(),
				VolumeContext: map[string]string{
					"share_id":   shareID,
					"share_name": volumeName,
					"pool_name":  d.client.PoolName(),
				},
			}}
			goto owned
		}

		// Validate content-source identity before any create/clone mutation. NGX
		// supports snapshot clone for shares but not direct share-to-share clone.
		if source := req.GetVolumeContentSource(); source != nil {
			switch typed := source.GetType().(type) {
			case *csi.VolumeContentSource_Snapshot:
				snapshotSourceID = typed.Snapshot.GetSnapshotId()
				if snapshotSourceID == "" {
					d.log().Error("CreateVolume(): source snapshot ID is required")
					criticalErr = status.Error(codes.InvalidArgument, "source snapshot ID is required")
					goto release
				}
				snapshots, err := d.client.Snapshots().ListDetail(ctx, "")
				if err != nil {
					d.log().Error("CreateVolume(): source snapshot list failed",
						zap.String("snapshot_id", snapshotSourceID),
						zap.Error(err),
					)
					criticalErr = utils.BackendStatusError("list source snapshots", err)
					goto release
				}
				found := false
				for i := range snapshots {
					if snapshots[i].ID == snapshotSourceID {
						found = true
						break
					}
				}
				if !found {
					d.log().Error("CreateVolume(): source snapshot was not found",
						zap.String("snapshot_id", snapshotSourceID),
					)
					criticalErr = status.Error(codes.NotFound, "source snapshot not found")
					goto release
				}
			case *csi.VolumeContentSource_Volume:
				if typed.Volume.GetVolumeId() == "" {
					d.log().Error("CreateVolume(): source volume ID is required")
					criticalErr = status.Error(codes.InvalidArgument, "source volume ID is required")
					goto release
				}
				d.log().Warn("CreateVolume(): direct volume cloning is unsupported",
					zap.String("source_volume_id", typed.Volume.GetVolumeId()),
				)
				criticalErr = status.Error(codes.Unimplemented, "NGX Storage does not support direct volume cloning")
				goto release
			default:
				d.log().Error("CreateVolume(): content source type is invalid",
					zap.Reflect("content_source", source),
				)
				criticalErr = status.Error(codes.InvalidArgument, "volume content source type is required")
				goto release
			}
		}

		// Check the configured pool's current logical.available bytes before
		// creating or expanding the resulting share.
		availableCapacity, err := d.GetCapacity(
			ctx,
			&csi.GetCapacityRequest{
				VolumeCapabilities: req.GetVolumeCapabilities(),
				Parameters:         req.GetParameters(),
			},
		)
		if err != nil {
			// GetCapacity already returns a CSI status error with the correct
			// readiness or pool failure classification.
			d.log().Error("CreateVolume(): capacity lookup failed", zap.Error(err))
			criticalErr = err
			goto release
		}

		if availableCapacity.GetAvailableCapacity() < capacityRange.GetRequiredBytes() {
			d.log().Error("CreateVolume(): pool capacity is insufficient",
				zap.Int64("required_bytes", capacityRange.GetRequiredBytes()),
				zap.Int64("available_bytes", availableCapacity.GetAvailableCapacity()),
			)
			criticalErr = status.Errorf(
				codes.ResourceExhausted,
				"requested %d bytes, available %d bytes",
				capacityRange.GetRequiredBytes(),
				availableCapacity.GetAvailableCapacity(),
			)
			goto release
		}
		if err := lockSet.AssertOwned(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				criticalErr = status.FromContextError(err).Err()
				goto release
			}
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify NFS create-volume lock ownership", err)
			goto release
		}

		// Mutate exactly one backend resource: clone a validated snapshot or create
		// a fresh share with its NFS export disabled.
		var createdShare *ngxstorage.Share
		if snapshotSourceID != "" {
			shareID, err := d.client.Snapshots().Clone(ctx, snapshotSourceID, volumeName)
			if err != nil {
				d.log().Error("CreateVolume(): snapshot clone failed",
					zap.String("snapshot_id", snapshotSourceID),
					zap.Error(err),
				)
				criticalErr = utils.BackendStatusError("clone snapshot", err)
				goto release
			}
			// Clone shares inherit the source snapshot's soft_quota. Expand to the
			// requested quota only when it is larger than the inherited quota; a
			// shrink attempt would be rejected by the backend.
			cloned, err := d.client.Shares().Get(ctx, shareID)
			if err != nil {
				d.log().Error("CreateVolume(): cloned share lookup failed",
					zap.String("volume_id", shareID),
					zap.Error(err),
				)
				criticalErr = utils.BackendStatusError("lookup cloned share", err)
				goto release
			}
			if cloned == nil || cloned.ID != shareID || cloned.Name != volumeName {
				d.log().Error("CreateVolume(): cloned share readback has invalid identity",
					zap.String("expected_volume_id", shareID),
					zap.String("expected_name", volumeName),
					zap.Reflect("readback", cloned),
				)
				criticalErr = status.Error(codes.Internal, "clone snapshot: cloned share readback identity mismatch")
				goto release
			}
			inheritedBytes := helper.ShareSoftQuotaBytes(cloned)
			if inheritedBytes > 0 && capacityRange.GetRequiredBytes() > inheritedBytes {
				if err := d.client.Shares().Expand(ctx, shareID, capacityRange.GetRequiredBytes()); err != nil {
					d.log().Error("CreateVolume(): cloned share quota update failed",
						zap.String("volume_id", shareID),
						zap.Int64("soft_quota_bytes", capacityRange.GetRequiredBytes()),
						zap.Error(err),
					)
					criticalErr = utils.BackendStatusError("set cloned share quota", err)
					goto release
				}
			}
			createdShare = &ngxstorage.Share{ID: shareID, Name: volumeName}
		} else {
			createdShare, err = d.reconciler().CreateShareReconciled(ctx, createRequest)
			if err != nil {
				d.log().Error("CreateVolume(): backend share create failed",
					zap.String("name", volumeName),
					zap.Error(err),
				)
				criticalErr = utils.BackendStatusError("create share", err)
				goto release
			}
			if createdShare == nil || createdShare.ID == "" || createdShare.Name != volumeName {
				d.log().Error("CreateVolume(): created share readback has invalid identity",
					zap.String("expected_name", volumeName),
					zap.Reflect("readback", createdShare),
				)
				criticalErr = status.Error(codes.Internal, "create share: backend readback identity mismatch")
				goto release
			}
		}
		if permissions := parameters["permissions"]; permissions != "" {
			if err := lockSet.AssertOwned(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					criticalErr = status.FromContextError(err).Err()
				} else {
					criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify NFS create-volume ownership before setting permissions", err)
				}
				goto release
			}
			if err := d.reconciler().ModifyShareOnOwner(ctx, createdShare.ID, map[string]interface{}{"permissions": permissions}); err != nil {
				criticalErr = utils.BackendStatusError("set share permissions", err)
				goto release
			}
		}
		d.volumeHashes.Track(createdShare)

		// Return the canonical backend ID and immutable context consumed by later
		// publish and node operations.
		d.log().Info("CreateVolume(): volume created",
			zap.String("volume_id", createdShare.ID),
			zap.String("name", volumeName),
			zap.Int64("capacity_bytes", capacityRange.GetRequiredBytes()),
			zap.String("source_snapshot_id", snapshotSourceID),
		)
		result = &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      createdShare.ID,
				CapacityBytes: capacityRange.GetRequiredBytes(),
				ContentSource: req.GetVolumeContentSource(),
				VolumeContext: map[string]string{
					"share_id":   createdShare.ID,
					"share_name": volumeName,
					"pool_name":  d.client.PoolName(),
				},
			},
		}
		goto owned
	}
owned:
	if ownedErr := lockSet.AssertOwned(ctx); ownedErr != nil {
		if errors.Is(ownedErr, context.Canceled) || errors.Is(ownedErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(ownedErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS create-volume "+lockNoun+" ownership", ownedErr)
		}
	}
release:
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), coordinationReleaseTimeout)
	releaseErr := lockSet.Release(releaseCtx)
	releaseCancel()
	if criticalErr != nil {
		if releaseErr != nil {
			if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
				return nil, errors.Join(criticalErr, status.FromContextError(releaseErr).Err())
			}
			return nil, errors.Join(criticalErr, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS create-volume "+lockNoun, releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(releaseErr).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS create-volume "+lockNoun, releaseErr)
	}
	return result, nil
}

/*
 * DeleteVolume deletes a dynamically provisioned NGX share by canonical ID.
 * Missing shares are idempotent successes. Static handles in the form
 * nfs.csi.ngxstorage.com.<share-name> are administrator-owned and are retained.
 * Kubernetes reclaimPolicy determines whether the CO invokes this RPC.
 *
 * Backend:
 *   - Reads GET /api/v2/share/{id} on every dynamic delete request.
 *   - Deletes through DELETE /api/v2/share/{id}.
 *   - Uses canonical id and name fields.
 * The RPC does not require, start, stop, or modify the NFS service.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id. The ID may be a dynamic backend ID or a
 *             static existing-share handle.
 *
 * @return *csi.DeleteVolumeResponse: Empty response after confirmed deletion,
 *         static-share retention, or an already-absent backend share.
 * @return error: InvalidArgument for a missing ID, Unavailable when storage is
 *         temporarily unavailable, and Internal for non-idempotent backend
 *         failures.
 */
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	d.log().Info("DeleteVolume(): init")
	d.log().Debug("DeleteVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Validate context and required identity before checking backend ownership.
	if err := ctx.Err(); err != nil {
		d.log().Error("DeleteVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("DeleteVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	volumeID := req.GetVolumeId()
	d.log().Info("DeleteVolume(): request accepted", zap.String("volume_id", volumeID))

	// Static existing-share handles are references to administrator-owned
	// resources and must never trigger a backend delete.
	if strings.HasPrefix(volumeID, DriverName+".") {
		// Static handles identify pre-existing shares by name. They are mount
		// references, not backend IDs returned by CreateVolume.
		d.log().Info("DeleteVolume(): retaining static existing share",
			zap.String("volume_id", volumeID),
			zap.String("share_name", helper.ShareName(volumeID)),
		)
		return &csi.DeleteVolumeResponse{}, nil
	}
	if d.coordination == nil {
		return nil, status.Errorf(codes.Unavailable, "%s: coordinator is unavailable", "acquire NFS delete-volume lock")
	}
	lock, err := d.coordination.Acquire(ctx, "volume:"+volumeID, coord.AcquireOptions{
		Safety: coord.SafetyDestructive, Operation: coord.OperationDelete,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "acquire NFS delete-volume lock", err)
	}
	var result *csi.DeleteVolumeResponse
	criticalErr := lock.AssertOwned(ctx)
	if criticalErr != nil {
		if errors.Is(criticalErr, context.Canceled) || errors.Is(criticalErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(criticalErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS delete-volume lock ownership", criticalErr)
		}
		goto release
	}
	{

		// Resolve the canonical backend record on every call. Process-local state
		// cannot prove existence or ownership after a controller restart.
		share, err := d.client.Shares().Get(ctx, volumeID)
		if err != nil {
			if utils.ClassifyError(err) == utils.ErrorNotFound {
				d.log().Info("DeleteVolume(): backend share is already absent",
					zap.String("volume_id", volumeID),
				)
				result = &csi.DeleteVolumeResponse{}
				goto owned
			}
			d.log().Error("DeleteVolume(): backend lookup failed",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("delete volume lookup", err)
			goto release
		}
		d.log().Debug("DeleteVolume(): backend share resolved",
			zap.String("volume_id", volumeID),
			zap.String("share_name", share.Name),
		)
		if err := lock.AssertOwned(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				criticalErr = status.FromContextError(err).Err()
				goto release
			}
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify NFS delete-volume lock ownership", err)
			goto release
		}

		// Delete by canonical backend ID. The SDK treats a concurrently deleted
		// share as an idempotent success.
		if err := d.client.Shares().Delete(ctx, volumeID); err != nil {
			d.log().Error("DeleteVolume(): backend delete failed",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("delete share", err)
			goto release
		}
		// Read-after-delete: a 200-success delete response is not proof of
		// backend absence. Verify authoritative NotFound before claiming CSI
		// success or completing bookkeeping; a still-present share (e.g. the
		// array silently no-ops a delete while a snapshot clone depends on it)
		// must surface as an error so the caller retries. No snapshot/clone
		// cascade is attempted — dependent objects are never deleted here.
		if err := d.reconciler().VerifyShareDeleted(ctx, volumeID); err != nil {
			d.log().Error("DeleteVolume(): delete verification failed",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
			criticalErr = err
			goto release
		}
		d.volumeHashes.Untrack(share)
		d.log().Info("DeleteVolume(): share deleted (backend absence verified)",
			zap.String("volume_id", volumeID),
			zap.String("share_name", share.Name),
		)
		result = &csi.DeleteVolumeResponse{}
		goto owned
	}
owned:
	if ownedErr := lock.AssertOwned(ctx); ownedErr != nil {
		if errors.Is(ownedErr, context.Canceled) || errors.Is(ownedErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(ownedErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS delete-volume lock ownership", ownedErr)
		}
	}
release:
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), coordinationReleaseTimeout)
	releaseErr := lock.Release(releaseCtx)
	releaseCancel()
	if criticalErr != nil {
		if releaseErr != nil {
			if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
				return nil, errors.Join(criticalErr, status.FromContextError(releaseErr).Err())
			}
			return nil, errors.Join(criticalErr, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS delete-volume lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(releaseErr).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS delete-volume lock", releaseErr)
	}
	return result, nil
}

/*
 * ValidateVolumeCapabilities confirms that an NGX share exists and that every
 * requested CSI capability is supported by this NFS driver.
 * This RPC is read-only, does not acquire the mutation lock, and never treats
 * process-local share maps as the source of truth.
 *
 * Backend:
 *   - Dynamic IDs read GET /api/v2/share/{id}.
 *   - Static nfs.csi.ngxstorage.com.<share-name> handles read the canonical
 *     GET /api/v2/share/list response and match only the name field.
 *   - Uses canonical id and name fields without compatibility aliases.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id and one or more mount volume_capabilities.
 *             Volume context and parameter maps are echoed only after every
 *             capability is confirmed.
 *
 * @return *csi.ValidateVolumeCapabilitiesResponse: Sets Confirmed for fully
 *         supported capabilities. Unsupported capabilities return OK with an
 *         empty Confirmed field and a diagnostic Message as required by CSI.
 * @return error: InvalidArgument for missing required fields, NotFound for an
 *         absent share, Unavailable for temporary storage failure, and
 *         Internal for malformed canonical backend identity.
 */
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	d.log().Info("ValidateVolumeCapabilities(): init")
	d.log().Debug("ValidateVolumeCapabilities(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.Reflect("volume_capabilities", req.GetVolumeCapabilities()),
		zap.Reflect("volume_context", req.GetVolumeContext()),
		zap.Reflect("parameters", req.GetParameters()),
		zap.Reflect("mutable_parameters", req.GetMutableParameters()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Validate the read-only request before resolving any backend share.
	if err := ctx.Err(); err != nil {
		d.log().Error("ValidateVolumeCapabilities(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("ValidateVolumeCapabilities(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		d.log().Error("ValidateVolumeCapabilities(): volume capabilities are required",
			zap.String("volume_id", req.GetVolumeId()),
		)
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	volumeID := req.GetVolumeId()
	d.log().Info("ValidateVolumeCapabilities(): request accepted",
		zap.String("volume_id", volumeID),
		zap.Reflect("volume_capabilities", req.GetVolumeCapabilities()),
	)

	var (
		share     *ngxstorage.Share
		shareName string
	)

	// Resolve static handles by canonical name and dynamic handles by canonical
	// ID. Process-local cache state is intentionally not consulted.
	if strings.HasPrefix(volumeID, DriverName+".") {
		shareName = helper.ShareName(volumeID)
		d.log().Debug("ValidateVolumeCapabilities(): resolving static existing share",
			zap.String("volume_id", volumeID),
			zap.String("share_name", shareName),
		)
		shares, err := d.client.Shares().List(ctx)
		if err != nil {
			d.log().Error("ValidateVolumeCapabilities(): share list failed",
				zap.String("share_name", shareName),
				zap.Error(err),
			)
			return nil, utils.BackendStatusError("validate volume lookup", err)
		}
		for i := range shares {
			if shares[i].Name == shareName {
				share = &shares[i]
				break
			}
		}
		if share == nil {
			d.log().Error("ValidateVolumeCapabilities(): static share was not found",
				zap.String("share_name", shareName),
			)
			return nil, status.Errorf(codes.NotFound, "volume %s was not found", volumeID)
		}
		if share.ID == "" {
			d.log().Error("ValidateVolumeCapabilities(): static share response is missing id",
				zap.String("volume_id", volumeID),
				zap.String("share_name", shareName),
			)
			return nil, status.Error(codes.Internal, "validate volume: static backend response is missing id")
		}
	} else {
		var err error
		share, err = d.client.Shares().Get(ctx, volumeID)
		if err != nil {
			d.log().Error("ValidateVolumeCapabilities(): backend lookup failed",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
			return nil, utils.BackendStatusError("validate volume lookup", err)
		}
		if share.ID == "" || share.ID != volumeID {
			d.log().Error("ValidateVolumeCapabilities(): backend returned an invalid share ID",
				zap.String("requested_volume_id", volumeID),
				zap.String("backend_volume_id", share.ID),
			)
			return nil, status.Error(codes.Internal, "validate volume: backend response has an invalid id")
		}
		shareName = share.Name
		if shareName == "" {
			d.log().Error("ValidateVolumeCapabilities(): backend response is missing name",
				zap.String("volume_id", volumeID),
			)
			return nil, status.Error(codes.Internal, "validate volume: backend response is missing name")
		}
	}

	d.log().Debug("ValidateVolumeCapabilities(): backend volume resolved",
		zap.String("volume_id", volumeID),
		zap.String("share_name", shareName),
	)

	// CSI requires unsupported capabilities to return OK without Confirmed,
	// while malformed request fields and lookup failures remain gRPC errors.
	for index, capability := range req.GetVolumeCapabilities() {
		if err := utils.ValidateVolumeCapability(capability, d.AccessModes); err != nil {
			message := fmt.Sprintf("capability %d is not supported: %v", index, err)
			d.log().Info("ValidateVolumeCapabilities(): capability is not supported",
				zap.String("volume_id", volumeID),
				zap.Int("capability_index", index),
				zap.Error(err),
			)
			return &csi.ValidateVolumeCapabilitiesResponse{Message: message}, nil
		}
	}

	// Echo only the fields validated for this canonical backend share.
	d.log().Info("ValidateVolumeCapabilities(): capabilities confirmed",
		zap.String("volume_id", volumeID),
		zap.Reflect("volume_capabilities", req.GetVolumeCapabilities()),
	)
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
			VolumeContext:      req.VolumeContext,
			Parameters:         req.Parameters,
			MutableParameters:  req.MutableParameters,
		},
	}, nil
}

/*
 * GetCapacity returns the configured NGX pool's logical.available value.
 * Capacity is returned in bytes. The RPC is read-only: it does not reserve
 * space, apply an undocumented safety percentage, or invent minimum/maximum
 * volume sizes absent from the backend contract.
 *
 * Backend:
 *   - Reads GET /api/v2/status/capacity.
 *   - Selects the configured pool by canonical name.
 *   - Reads logical.available and logical.reserved as byte values.
 * Controller and pool selection is refreshed centrally after five minutes and
 * once after a transport failure; semantic API errors are not replayed.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: May contain mount volume_capabilities, parameters, and accessible
 *             topology. An omitted request is treated as an empty request.
 *
 * @return *csi.GetCapacityResponse: Returns available_capacity in bytes.
 *         Unsupported capabilities return an OK response with zero capacity.
 * @return error: Unavailable for cluster/transport failures, NotFound for a
 *         missing configured pool, and Internal for malformed capacity data.
 */
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	d.log().Info("GetCapacity(): init")
	d.log().Debug("GetCapacity(): request",
		zap.Bool("request_present", req != nil),
		zap.Reflect("volume_capabilities", req.GetVolumeCapabilities()),
		zap.Reflect("parameters", req.GetParameters()),
		zap.Reflect("accessible_topology", req.GetAccessibleTopology()),
	)

	// Treat an omitted request as an empty filter after honoring cancellation.
	if err := ctx.Err(); err != nil {
		d.log().Error("GetCapacity(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil {
		req = &csi.GetCapacityRequest{}
	}
	d.log().Info("GetCapacity(): request accepted",
		zap.Reflect("volume_capabilities", req.GetVolumeCapabilities()),
		zap.Reflect("parameters", req.GetParameters()),
		zap.Reflect("accessible_topology", req.GetAccessibleTopology()),
	)

	// Unsupported capabilities have no usable capacity but are not malformed
	// GetCapacity requests, so CSI expects an OK response with zero bytes.
	for index, capability := range req.GetVolumeCapabilities() {
		if err := utils.ValidateVolumeCapability(capability, d.AccessModes); err != nil {
			d.log().Info("GetCapacity(): requested capability has no usable capacity",
				zap.Int("capability_index", index),
				zap.Error(err),
			)
			return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
		}
	}

	// Read the configured pool's canonical logical byte values. Controller and
	// pool refresh/failover are centralized inside the SDK client.
	availableCapacity, reservedCapacity, err := d.client.Pools().GetConfiguredCapacity(ctx)
	if err != nil {
		d.log().Error("GetCapacity(): pool capacity lookup failed", zap.Error(err))
		return nil, utils.BackendStatusError("get capacity", err)
	}

	// Return only backend-confirmed available bytes; no local safety percentage
	// or guessed limits are applied.
	d.log().Info("GetCapacity(): capacity returned",
		zap.Int64("available_bytes", availableCapacity),
		zap.Int64("reserved_bytes", reservedCapacity),
	)
	d.log().Debug("GetCapacity(): response",
		zap.Int64("available_capacity", availableCapacity),
	)
	return &csi.GetCapacityResponse{AvailableCapacity: availableCapacity}, nil
}

/*
 * ControllerGetCapabilities returns the controller RPC capabilities currently
 * advertised by this driver. It is an in-memory read and performs no backend
 * request, mutation, retry, or locking.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Empty CSI request.
 *
 * @return *csi.ControllerGetCapabilitiesResponse: Returns the exact capability
 *         list configured during driver initialization.
 * @return error: Canceled or DeadlineExceeded when the context has ended.
 */
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	d.log().Info("ControllerGetCapabilities(): init")
	d.log().Debug("ControllerGetCapabilities(): request",
		zap.Bool("request_present", req != nil),
	)

	// This in-memory RPC still honors the caller's deadline.
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerGetCapabilities(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	// Return exactly the capabilities configured during NewDriver.
	d.log().Info("ControllerGetCapabilities(): capabilities returned",
		zap.Reflect("capabilities", d.csCap),
	)
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: d.csCap}, nil
}

/*
 * ListVolumes returns canonical NGX shares using CSI pagination semantics.
 * The backend list is the source of truth; process-local hashes and mutation
 * locks are not used. Tokens are stable canonical IDs prefixed with v1:.
 *
 * Backend:
 *   - Reads GET /api/v2/share/list.
 *   - Uses canonical id and capacity.soft_quota fields.
 *   - Treats soft_quota as bytes and skips malformed or incomplete records so
 *     one damaged backend share cannot hide the remaining volume list.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Accepts max_entries >= 0 and an optional opaque starting_token.
 *             An omitted request is treated as an empty request.
 *
 * @return *csi.ListVolumesResponse: Returns sorted volume entries and a
 *         next_token when more canonical shares remain.
 * @return error: InvalidArgument for negative max_entries, Aborted for an
 *         invalid token, Unavailable for temporary backend failure, and
 *         Internal for backend records that are impossible to interpret.
 */
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	d.log().Info("ListVolumes(): init")
	d.log().Debug("ListVolumes(): request",
		zap.Bool("request_present", req != nil),
		zap.Int32("max_entries", req.GetMaxEntries()),
		zap.String("starting_token", req.GetStartingToken()),
	)

	// Normalize an omitted request and reject malformed pagination before the
	// backend list operation.
	if err := ctx.Err(); err != nil {
		d.log().Error("ListVolumes(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil {
		req = &csi.ListVolumesRequest{}
	}
	if req.GetMaxEntries() < 0 {
		d.log().Error("ListVolumes(): max entries cannot be negative",
			zap.Int32("max_entries", req.GetMaxEntries()),
		)
		return nil, status.Error(codes.InvalidArgument, "max entries cannot be negative")
	}
	d.log().Info("ListVolumes(): request accepted",
		zap.Int32("max_entries", req.GetMaxEntries()),
		zap.Bool("has_starting_token", req.GetStartingToken() != ""),
	)

	// Build CSI entries only from canonical backend records; local share maps
	// cannot provide a complete list after restart.
	shares, err := d.client.Shares().List(ctx)
	if err != nil {
		d.log().Error("ListVolumes(): backend share list failed", zap.Error(err))
		return nil, utils.BackendStatusError("list volumes", err)
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, len(shares))
	for i := range shares {
		share := shares[i]
		volumeID := share.ID
		if volumeID == "" {
			d.log().Error("ListVolumes(): backend record is missing id, skipping")
			continue
		}
		capacityBytes := helper.ShareSoftQuotaBytes(&share)
		if capacityBytes <= 0 {
			d.log().Error("ListVolumes(): backend record has invalid soft quota, skipping",
				zap.String("volume_id", volumeID),
			)
			continue
		}
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: capacityBytes,
			},
		})
	}

	// Sort by stable backend ID so cursor tokens remain deterministic.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].GetVolume().GetVolumeId() < entries[j].GetVolume().GetVolumeId()
	})

	start := 0
	const tokenPrefix = "v1:"
	if token := req.GetStartingToken(); token != "" {
		if !strings.HasPrefix(token, tokenPrefix) || len(token) == len(tokenPrefix) {
			d.log().Error("ListVolumes(): invalid starting token")
			return nil, status.Error(codes.Aborted, "invalid starting token")
		}
		cursor := strings.TrimPrefix(token, tokenPrefix)
		for start < len(entries) && entries[start].GetVolume().GetVolumeId() <= cursor {
			start++
		}
	}

	end := len(entries)
	if maxEntries := req.GetMaxEntries(); maxEntries > 0 && int64(start)+int64(maxEntries) < int64(end) {
		end = start + int(maxEntries)
	}

	// Slice the sorted result and derive the next cursor from the final returned
	// canonical ID.
	response := &csi.ListVolumesResponse{Entries: entries[start:end]}
	if end < len(entries) {
		response.NextToken = tokenPrefix + entries[end-1].GetVolume().GetVolumeId()
	}
	d.log().Info("ListVolumes(): volumes returned",
		zap.Reflect("entries", response.Entries),
		zap.String("next_token", response.NextToken),
	)
	d.log().Debug("ListVolumes(): response",
		zap.Reflect("entries", response.Entries),
		zap.String("next_token", response.NextToken),
	)
	return response, nil
}

/*
 * ControllerPublishVolume verifies an NGX share and enables its NFS export.
 * Repeating a compatible publish is idempotent. Static existing-share handles
 * are administrator-owned: their export must already be enabled and is never
 * modified by this RPC.
 *
 * Backend:
 *   - Dynamic IDs read GET /api/v2/share/{id}.
 *   - Static handles read GET /api/v2/share/list and match canonical name.
 *   - Dynamic disabled exports write PATCH /api/v2/share/{id} with
 *     nfs_export=true.
 *   - Reads canonical id, name, pool_name, and exports.nfs.enabled.
 * NFS service version settings are never opened or modified automatically.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id, node_id, and a supported mount
 *             volume_capability. readonly selects the share-global export
 *             read-only state; a republish with a different state is
 *             incompatible (AlreadyExists).
 *
 * @return *csi.ControllerPublishVolumeResponse: Returns non-sensitive server
 *         and export path values in publish_context.
 * @return error: InvalidArgument for malformed CSI fields, NotFound for an
 *         absent share or an unknown node, FailedPrecondition for a disabled
 *         static export, AlreadyExists for an incompatible read-only
 *         republish, Unavailable for temporary backend failure, and
 *         Internal for malformed canonical export data.
 */
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	d.log().Info("ControllerPublishVolume(): init")
	d.log().Debug("ControllerPublishVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("node_id", req.GetNodeId()),
		zap.Bool("readonly", req.GetReadonly()),
		zap.Reflect("volume_capability", req.GetVolumeCapability()),
		zap.Reflect("volume_context", req.GetVolumeContext()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Validate all CSI publish fields before resolving or changing an export.
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerPublishVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil {
		d.log().Error("ControllerPublishVolume(): request is required")
		return nil, status.Error(codes.InvalidArgument, "controller publish request is required")
	}
	if req.GetVolumeId() == "" || req.GetNodeId() == "" {
		d.log().Error("ControllerPublishVolume(): volume ID and node ID are required",
			zap.String("volume_id", req.GetVolumeId()),
			zap.String("node_id", req.GetNodeId()),
		)
		return nil, status.Error(codes.InvalidArgument, "volume ID and node ID are required")
	}
	if err := utils.ValidateVolumeCapability(req.GetVolumeCapability(), d.AccessModes); err != nil {
		d.log().Error("ControllerPublishVolume(): volume capability is invalid",
			zap.Reflect("volume_capability", req.GetVolumeCapability()),
			zap.Error(err),
		)
		return nil, err
	}

	// A node-role process serves exactly one node: reject publishes addressed
	// to another node before any backend state is resolved or changed. The
	// controller role resolves node IDs through the Kubernetes API below.
	if d.Role != "controller" && req.GetNodeId() != d.NodeId {
		d.log().Error("ControllerPublishVolume(): node was not found",
			zap.String("node_id", req.GetNodeId()),
		)
		return nil, status.Errorf(codes.NotFound, "node %s was not found", req.GetNodeId())
	}
	// The controller role resolves the node through the Kubernetes API; an
	// unknown node is NotFound before any export state changes.
	if d.Role != "node" && d.nodeLookup != nil {
		exists, err := d.nodeLookup.NodeExists(ctx, req.GetNodeId())
		if err != nil {
			d.log().Error("ControllerPublishVolume(): node lookup failed",
				zap.String("node_id", req.GetNodeId()), zap.Error(err))
			return nil, status.Errorf(codes.Unavailable, "lookup node %s: %v", req.GetNodeId(), err)
		}
		if !exists {
			d.log().Error("ControllerPublishVolume(): node was not found",
				zap.String("node_id", req.GetNodeId()))
			return nil, status.Errorf(codes.NotFound, "node %s was not found", req.GetNodeId())
		}
	}

	volumeID := req.GetVolumeId()
	staticVolume := strings.HasPrefix(volumeID, DriverName+".")
	d.log().Info("ControllerPublishVolume(): request accepted",
		zap.String("volume_id", volumeID),
		zap.String("node_id", req.GetNodeId()),
		zap.Bool("static_volume", staticVolume),
	)

	publishWithCanonicalState := func(lock coord.Lock) (*csi.ControllerPublishVolumeResponse, error) {
		share, err := d.reconciler().ResolvePublishShare(ctx, volumeID, staticVolume)
		if err != nil {
			return nil, err
		}
		backendID := share.ID

		// Parse only the canonical identity and export fields documented by the
		// released share-detail schema.
		shareName := share.Name
		poolName := share.PoolName
		if shareName == "" || poolName == "" {
			d.log().Error("ControllerPublishVolume(): backend response missing share identity",
				zap.String("volume_id", volumeID),
				zap.String("share_name", shareName),
				zap.String("pool_name", poolName),
			)
			return nil, status.Error(codes.Internal, "publish volume: backend response missing name or pool_name")
		}

		// The SDK decodes the canonical exports.nfs document; a missing or
		// disabled document decodes to the zero (disabled) state. Static shares
		// with a disabled export are rejected below; dynamic shares are enabled
		// through the idempotent PATCH.
		enabled := bool(share.Exports.NFS.Enabled)
		currentReadOnly := bool(share.Exports.NFS.ReadOnly)

		// Static export policy is administrator-owned. The driver may verify it but
		// must not enable a disabled pre-existing share.
		if staticVolume && !enabled {
			d.log().Info("ControllerPublishVolume(): static share export is disabled",
				zap.String("volume_id", volumeID),
				zap.String("share_name", shareName),
			)
			return nil, status.Error(codes.FailedPrecondition, "pre-existing share NFS export is disabled")
		}

		// The export read-only state is share-global and fixed at publish time.
		// A publish requesting a different read-only state than the already
		// enabled export is incompatible with the existing publish and must
		// fail AlreadyExists (csi-sanity "already published but incompatible").
		wantReadOnly := req.GetReadonly()
		if enabled && currentReadOnly != wantReadOnly {
			d.log().Error("ControllerPublishVolume(): volume is already published with incompatible read-only state",
				zap.String("volume_id", volumeID),
				zap.Bool("current_read_only", currentReadOnly),
				zap.Bool("requested_read_only", wantReadOnly),
			)
			return nil, status.Error(codes.AlreadyExists, "volume is already published with incompatible read-only state")
		}

		// Enable the dynamic share export and align its read-only state to the
		// requested mode in a single atomic PATCH. The backend ignores an
		// nfs_read_only write while the export is disabled, and an all-node
		// unpublish leaves the stale read-only flag set when it disables the
		// export — so enabling first and setting the mode separately would re-enable
		// a share that is still read-only for a writable publish (writes then fail).
		// Shipping both flags together fixes both directions (RO→RW and RW→RO) in
		// one call with no transient writable window. Dynamic controller export
		// state is serialized by the distributed volume lock, not by node-local
		// bookkeeping.
		if !enabled {
			if !staticVolume {
				if err := lock.AssertOwned(ctx); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return nil, status.FromContextError(err).Err()
					}
					return nil, status.Errorf(codes.Unavailable, "%s: %v", "verify NFS publish-volume lock ownership", err)
				}
			}
			// A publish can race the CreateShare commit on the backend; the
			// API answers that transient state with a retryable error
			// (code=503 "please try again"). Retrying a few times here keeps
			// the external-attacher from failing the VolumeAttachment and
			// backing off for minutes.
			var setErr error
			for attempt := 1; attempt <= 5; attempt++ {
				setErr = d.reconciler().ModifyShareOnOwner(ctx, backendID, map[string]interface{}{
					"nfs_export":    true,
					"nfs_read_only": wantReadOnly,
				})
				if setErr == nil {
					break
				}
				d.log().Warn("ControllerPublishVolume(): enabling NFS export failed, retrying",
					zap.String("volume_id", volumeID),
					zap.String("backend_id", backendID),
					zap.Int("attempt", attempt),
					zap.Error(setErr),
				)
				if attempt < 5 {
					select {
					case <-ctx.Done():
						return nil, status.FromContextError(ctx.Err()).Err()
					case <-time.After(2 * time.Second):
					}
				}
			}
			if setErr != nil {
				d.log().Error("ControllerPublishVolume(): enabling NFS export failed",
					zap.String("volume_id", volumeID),
					zap.String("backend_id", backendID),
					zap.Error(setErr),
				)
				return nil, utils.BackendStatusError("publish volume", setErr)
			}
			d.log().Info("ControllerPublishVolume(): NFS export enabled",
				zap.String("volume_id", volumeID),
				zap.String("backend_id", backendID),
				zap.Bool("read_only", wantReadOnly),
			)
		} else {
			d.log().Debug("ControllerPublishVolume(): NFS export already enabled",
				zap.String("volume_id", volumeID),
				zap.Bool("read_only", currentReadOnly),
			)
		}

		// Return stable, non-sensitive NFS coordinates for the node-side mount.
		publishContext := map[string]string{
			"server": d.dataIP,
			"path":   path.Join(d.dataPath, poolName, shareName),
		}
		d.log().Info("ControllerPublishVolume(): volume published",
			zap.String("volume_id", volumeID),
			zap.String("node_id", req.GetNodeId()),
			zap.Reflect("publish_context", publishContext),
		)
		return &csi.ControllerPublishVolumeResponse{PublishContext: publishContext}, nil
	}

	// Static exports are administrator-owned and never mutated, so they do not
	// participate in the controller Lease domain. Dynamic publish always takes
	// the share-global volume lock before resolving export/read-only state so an
	// all-node unpublish cannot invalidate a stale successful decision.
	if staticVolume {
		return publishWithCanonicalState(nil)
	}
	if d.coordination == nil {
		return nil, status.Errorf(codes.Unavailable, "%s: coordinator is unavailable", "acquire NFS publish-volume lock")
	}
	lock, err := d.coordination.Acquire(ctx, "volume:"+volumeID, coord.AcquireOptions{
		Safety: coord.SafetyOrdinary, Operation: coord.OperationPublish,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "acquire NFS publish-volume lock", err)
	}
	var result *csi.ControllerPublishVolumeResponse
	criticalErr := lock.AssertOwned(ctx)
	if criticalErr != nil {
		if errors.Is(criticalErr, context.Canceled) || errors.Is(criticalErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(criticalErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS publish-volume lock ownership", criticalErr)
		}
	} else {
		result, criticalErr = publishWithCanonicalState(lock)
		if criticalErr == nil {
			if ownedErr := lock.AssertOwned(ctx); ownedErr != nil {
				if errors.Is(ownedErr, context.Canceled) || errors.Is(ownedErr, context.DeadlineExceeded) {
					criticalErr = status.FromContextError(ownedErr).Err()
				} else {
					criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS publish-volume lock ownership", ownedErr)
				}
			}
		}
	}
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), coordinationReleaseTimeout)
	releaseErr := lock.Release(releaseCtx)
	releaseCancel()
	if criticalErr != nil {
		if releaseErr != nil {
			if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
				return nil, errors.Join(criticalErr, status.FromContextError(releaseErr).Err())
			}
			return nil, errors.Join(criticalErr, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS publish-volume lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(releaseErr).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS publish-volume lock", releaseErr)
	}
	return result, nil
}

/*
 * ControllerUnpublishVolume preserves a dynamically provisioned share's
 * global NFS export for node-specific requests and disables it only when CSI
 * omits node_id to request unpublish from all nodes. NodeUnpublishVolume owns
 * removal of the specified node's local mount. Missing shares and
 * already-disabled exports are idempotent successes. Static existing shares
 * are always retained because export policy belongs to the administrator.
 *
 * Backend:
 *   - Reads GET /api/v2/share/{id}.
 *   - Writes PATCH /api/v2/share/{id} with nfs_export=false.
 *   - Reads canonical id and exports.nfs.enabled.
 * NGX export state is share-global and exposes no restart-safe per-node
 * attachment records. A process-local reference counter is therefore not used.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id. node_id is optional under the CSI unpublish
 *             contract and is retained only as operation context.
 *
 * @return *csi.ControllerUnpublishVolumeResponse: Empty response after
 *         node-specific global-export retention, confirmed all-node disable,
 *         static-share retention, or idempotent absence.
 * @return error: InvalidArgument for a missing ID, Unavailable for temporary
 *         backend failure, and Internal for malformed export data.
 */
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	d.log().Info("ControllerUnpublishVolume(): init")
	d.log().Debug("ControllerUnpublishVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("node_id", req.GetNodeId()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Validate required identity before applying static ownership or backend
	// idempotency rules.
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerUnpublishVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("ControllerUnpublishVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	volumeID := req.GetVolumeId()
	d.log().Info("ControllerUnpublishVolume(): request accepted",
		zap.String("volume_id", volumeID),
		zap.String("node_id", req.GetNodeId()),
	)
	// Static shares retain administrator-owned export state.
	if strings.HasPrefix(volumeID, DriverName+".") {
		d.log().Info("ControllerUnpublishVolume(): retaining static share export",
			zap.String("volume_id", volumeID),
		)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	// CSI requires a populated node_id to affect only that node. NGX exposes
	// only share-global NFS enablement, so local NodeUnpublishVolume removes the
	// node mount while the controller preserves export access for other RWX
	// consumers. An empty node_id below is the CSI all-nodes operation.
	if req.GetNodeId() != "" {
		d.log().Info("ControllerUnpublishVolume(): retaining shared export for node-specific unpublish",
			zap.String("volume_id", volumeID),
			zap.String("node_id", req.GetNodeId()),
		)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if d.coordination == nil {
		return nil, status.Errorf(codes.Unavailable, "%s: coordinator is unavailable", "acquire NFS all-node unpublish lock")
	}
	lock, err := d.coordination.Acquire(ctx, "volume:"+volumeID, coord.AcquireOptions{
		Safety: coord.SafetyOrdinary, Operation: coord.OperationPublish,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "acquire NFS all-node unpublish lock", err)
	}
	var result *csi.ControllerUnpublishVolumeResponse
	criticalErr := lock.AssertOwned(ctx)
	if criticalErr != nil {
		if errors.Is(criticalErr, context.Canceled) || errors.Is(criticalErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(criticalErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS all-node unpublish lock ownership", criticalErr)
		}
		goto release
	}
	{

		// Re-read canonical export state only after taking the same global lock used
		// by ControllerPublishVolume. This closes publish-vs-all-node-unpublish stale
		// state races while leaving node-specific unpublish entirely lock-free.
		share, err := d.client.Shares().Get(ctx, volumeID)
		if err != nil {
			if utils.ClassifyError(err) == utils.ErrorNotFound {
				d.log().Info("ControllerUnpublishVolume(): share is already absent",
					zap.String("volume_id", volumeID),
				)
				result = &csi.ControllerUnpublishVolumeResponse{}
				goto owned
			}
			d.log().Error("ControllerUnpublishVolume(): backend lookup failed",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("unpublish volume lookup", err)
			goto release
		}
		if share.ID == "" || share.ID != volumeID {
			d.log().Error("ControllerUnpublishVolume(): backend response has invalid id",
				zap.String("volume_id", volumeID),
				zap.String("backend_id", share.ID),
			)
			criticalErr = status.Error(codes.Internal, "unpublish volume: backend response has an invalid id")
			goto release
		}

		// Read the canonical exports.nfs.enabled state; a disabled export is an
		// idempotent success.
		enabled := bool(share.Exports.NFS.Enabled)
		if !enabled {
			d.log().Debug("ControllerUnpublishVolume(): NFS export already disabled",
				zap.String("volume_id", volumeID),
			)
			result = &csi.ControllerUnpublishVolumeResponse{}
			goto owned
		}
		if err := lock.AssertOwned(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				criticalErr = status.FromContextError(err).Err()
				goto release
			}
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify NFS all-node unpublish lock ownership", err)
			goto release
		}

		// Disable only the NFS export for the all-nodes request. Node mounts and NFS
		// service settings remain outside this controller critical section.
		if err := d.client.Shares().SetExportEnabled(ctx, volumeID, false); err != nil {
			if utils.ClassifyError(err) == utils.ErrorNotFound {
				d.log().Info("ControllerUnpublishVolume(): share disappeared during unpublish",
					zap.String("volume_id", volumeID),
				)
				result = &csi.ControllerUnpublishVolumeResponse{}
				goto owned
			}
			d.log().Error("ControllerUnpublishVolume(): disabling NFS export failed",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("unpublish volume", err)
			goto release
		}
		d.log().Info("ControllerUnpublishVolume(): NFS export disabled",
			zap.String("volume_id", volumeID),
			zap.String("backend_id", volumeID),
		)
		result = &csi.ControllerUnpublishVolumeResponse{}
		goto owned
	}
owned:
	if ownedErr := lock.AssertOwned(ctx); ownedErr != nil {
		if errors.Is(ownedErr, context.Canceled) || errors.Is(ownedErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(ownedErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS all-node unpublish lock ownership", ownedErr)
		}
	}
release:
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), coordinationReleaseTimeout)
	releaseErr := lock.Release(releaseCtx)
	releaseCancel()
	if criticalErr != nil {
		if releaseErr != nil {
			if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
				return nil, errors.Join(criticalErr, status.FromContextError(releaseErr).Err())
			}
			return nil, errors.Join(criticalErr, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS all-node unpublish lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(releaseErr).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS all-node unpublish lock", releaseErr)
	}
	return result, nil
}

// GetSnapshot returns one snapshot by canonical ID. Context/input checks and
// gRPC result decisions live here; backend lookup/proto preparation is owned
// by the reconcile helper.
func (d *Driver) GetSnapshot(ctx context.Context, req *csi.GetSnapshotRequest) (*csi.GetSnapshotResponse, error) {
	d.log().Info("GetSnapshot(): init")
	if err := ctx.Err(); err != nil {
		d.log().Error("GetSnapshot(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetSnapshotId() == "" {
		d.log().Error("GetSnapshot(): snapshot ID is required")
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}
	snap, err := d.reconciler().SnapshotByID(ctx, req.GetSnapshotId())
	if err != nil {
		d.log().Error("GetSnapshot(): backend snapshot list failed", zap.Error(err))
		return nil, utils.BackendStatusError("list snapshots", err)
	}
	if snap == nil {
		d.log().Error("GetSnapshot(): snapshot not found",
			zap.String("snapshot_id", req.GetSnapshotId()),
		)
		return nil, status.Error(codes.NotFound, "snapshot not found")
	}
	return &csi.GetSnapshotResponse{Snapshot: snap}, nil
}

/*
 * CreateSnapshot creates an NGX snapshot of one source share.
 * Snapshot names are idempotent only when the existing snapshot has the same
 * canonical volume_id. Reusing a name for another source returns AlreadyExists.
 *
 * Backend:
 *   - Reads GET /api/v2/snapshot/list before mutation.
 *   - Creates through POST /api/v2/snapshot with name and volume_id.
 *   - Uses canonical id, name, volume_id, created, and reserved fields.
 * created is an epoch string converted to a CSI timestamp; reserved is bytes.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires name and source_volume_id.
 *
 * @return *csi.CreateSnapshotResponse: Returns snapshot_id, source_volume_id,
 *         size_bytes, creation_time, and ready_to_use.
 * @return error: InvalidArgument for missing fields, AlreadyExists for a name
 *         owned by another source, Unavailable for temporary backend failure,
 *         and Internal for malformed canonical snapshot data.
 */
func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	d.log().Info("CreateSnapshot(): init")
	d.log().Debug("CreateSnapshot(): request",
		zap.Bool("request_present", req != nil),
		zap.String("name", req.GetName()),
		zap.String("source_volume_id", req.GetSourceVolumeId()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Validate snapshot identity before any backend lookup or mutation.
	if err := ctx.Err(); err != nil {
		d.log().Error("CreateSnapshot(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetName() == "" {
		d.log().Error("CreateSnapshot(): snapshot name is required")
		return nil, status.Error(codes.InvalidArgument, "snapshot name is required")
	}
	if req.GetSourceVolumeId() == "" {
		d.log().Error("CreateSnapshot(): source volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "source volume ID is required")
	}
	d.log().Info("CreateSnapshot(): request accepted",
		zap.String("name", req.GetName()),
		zap.String("source_volume_id", req.GetSourceVolumeId()),
	)
	if d.coordination == nil {
		return nil, status.Errorf(codes.Unavailable, "%s: coordinator is unavailable", "acquire NFS create-snapshot locks")
	}
	lockSet, err := d.coordination.AcquireMany(ctx, []string{
		"snapshot-name:" + req.GetName(),
		"volume:" + req.GetSourceVolumeId(),
	}, coord.AcquireOptions{
		Safety: coord.SafetyOrdinary, Operation: coord.OperationSnapshot,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "acquire NFS create-snapshot locks", err)
	}
	var result *csi.CreateSnapshotResponse
	criticalErr := lockSet.AssertOwned(ctx)
	if criticalErr != nil {
		if errors.Is(criticalErr, context.Canceled) || errors.Is(criticalErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(criticalErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS create-snapshot locks ownership", criticalErr)
		}
		goto release
	}
	{

		// Enforce CSI idempotency and name uniqueness before calling the backend.
		// The name lookup spans all snapshots so reusing a name for another source
		// volume is detected even when the new source has no snapshots yet.
		snapshots, err := d.client.Snapshots().ListDetail(ctx, "")
		if err != nil {
			d.log().Error("CreateSnapshot(): snapshot list failed", zap.Error(err))
			criticalErr = utils.BackendStatusError("create snapshot idempotency lookup", err)
			goto release
		}
		for i := range snapshots {
			existing := snapshots[i]
			if existing.Name != req.GetName() {
				continue
			}
			// Convert the matching canonical record directly at the idempotency
			// branch so malformed fields retain CreateSnapshot-specific errors.
			snapshotID := existing.ID
			sourceVolumeID := existing.VolumeID
			created := existing.Created
			reserved := existing.Size
			if snapshotID == "" {
				d.log().Error("CreateSnapshot(): existing snapshot is missing id",
					zap.String("name", req.GetName()),
				)
				criticalErr = status.Error(codes.Internal, "create snapshot idempotency response missing id")
				goto release
			}
			if sourceVolumeID == "" {
				d.log().Error("CreateSnapshot(): existing snapshot is missing volume_id",
					zap.String("snapshot_id", snapshotID),
				)
				criticalErr = status.Error(codes.Internal, "create snapshot idempotency response missing volume_id")
				goto release
			}
			creationTime := utils.BackendCreatedTimestamp(created)
			if creationTime == nil {
				d.log().Error("CreateSnapshot(): existing snapshot has invalid created",
					zap.String("snapshot_id", snapshotID),
					zap.String("created", created),
				)
				criticalErr = status.Error(codes.Internal, "create snapshot idempotency response has invalid created")
				goto release
			}
			sizeBytes, err := strconv.ParseInt(strings.TrimSpace(reserved), 10, 64)
			if err != nil || sizeBytes < 0 {
				d.log().Error("CreateSnapshot(): existing snapshot has invalid reserved",
					zap.String("snapshot_id", snapshotID),
					zap.String("reserved", reserved),
					zap.Error(err),
				)
				criticalErr = status.Error(codes.Internal, "create snapshot idempotency response has invalid reserved")
				goto release
			}
			if sourceVolumeID != req.GetSourceVolumeId() {
				d.log().Info("CreateSnapshot(): name belongs to another source volume",
					zap.String("name", req.GetName()),
					zap.String("existing_source_volume_id", sourceVolumeID),
					zap.String("requested_source_volume_id", req.GetSourceVolumeId()),
				)
				criticalErr = status.Error(codes.AlreadyExists, "snapshot name already exists for another source volume")
				goto release
			}
			snapshot := &csi.Snapshot{
				SnapshotId:     snapshotID,
				SourceVolumeId: sourceVolumeID,
				SizeBytes:      sizeBytes,
				CreationTime:   creationTime,
				ReadyToUse:     true,
			}
			d.log().Info("CreateSnapshot(): existing snapshot returned",
				zap.String("snapshot_id", snapshot.GetSnapshotId()),
				zap.String("source_volume_id", snapshot.GetSourceVolumeId()),
				zap.Int64("size_bytes", snapshot.GetSizeBytes()),
			)
			result = &csi.CreateSnapshotResponse{Snapshot: snapshot}
			goto owned
		}

		// Create only after the canonical list proves the name is unused.
		if err := lockSet.AssertOwned(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				criticalErr = status.FromContextError(err).Err()
				goto release
			}
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify NFS create-snapshot lock ownership", err)
			goto release
		}
		created, err := d.client.Snapshots().Create(ctx, req.GetSourceVolumeId(), req.GetName())
		if err != nil {
			d.log().Error("CreateSnapshot(): backend create failed",
				zap.String("name", req.GetName()),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("create snapshot", err)
			goto release
		}
		if created == nil || created.ID == "" || created.Name != req.GetName() || created.VolumeID != req.GetSourceVolumeId() {
			d.log().Error("CreateSnapshot(): backend returned invalid snapshot identity",
				zap.String("expected_name", req.GetName()),
				zap.String("expected_source_volume_id", req.GetSourceVolumeId()),
				zap.Reflect("snapshot", created),
			)
			criticalErr = status.Error(codes.Internal, "create snapshot: backend returned invalid snapshot identity")
			goto release
		}
		sizeBytes, err := strconv.ParseInt(strings.TrimSpace(created.Size), 10, 64)
		if err != nil || sizeBytes < 0 {
			d.log().Error("CreateSnapshot(): backend returned invalid reserved",
				zap.String("snapshot_id", created.ID),
				zap.String("reserved", created.Size),
				zap.Error(err),
			)
			criticalErr = status.Error(codes.Internal, "create snapshot: backend returned invalid reserved")
			goto release
		}

		// Convert the confirmed canonical backend fields into the CSI snapshot
		// response; an invalid created epoch is not replaced with a guessed time.
		responseSnapshot := &csi.Snapshot{
			SnapshotId:     created.ID,
			SourceVolumeId: created.VolumeID,
			SizeBytes:      sizeBytes,
			ReadyToUse:     true,
		}
		if ts := utils.BackendCreatedTimestamp(created.Created); ts != nil {
			responseSnapshot.CreationTime = ts
		}
		if responseSnapshot.CreationTime == nil {
			d.log().Error("CreateSnapshot(): backend returned invalid creation time",
				zap.String("snapshot_id", created.ID),
				zap.String("created", created.Created),
			)
			criticalErr = status.Error(codes.Internal, "create snapshot: backend returned invalid creation time")
			goto release
		}
		d.log().Info("CreateSnapshot(): snapshot created",
			zap.String("snapshot_id", created.ID),
			zap.String("source_volume_id", created.VolumeID),
		)
		result = &csi.CreateSnapshotResponse{Snapshot: responseSnapshot}
		goto owned
	}
owned:
	if ownedErr := lockSet.AssertOwned(ctx); ownedErr != nil {
		if errors.Is(ownedErr, context.Canceled) || errors.Is(ownedErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(ownedErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS create-snapshot locks ownership", ownedErr)
		}
	}
release:
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), coordinationReleaseTimeout)
	releaseErr := lockSet.Release(releaseCtx)
	releaseCancel()
	if criticalErr != nil {
		if releaseErr != nil {
			if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
				return nil, errors.Join(criticalErr, status.FromContextError(releaseErr).Err())
			}
			return nil, errors.Join(criticalErr, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS create-snapshot locks", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(releaseErr).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS create-snapshot locks", releaseErr)
	}
	return result, nil
}

/*
 * DeleteSnapshot deletes an NGX snapshot by canonical ID.
 * An already-absent snapshot is an idempotent success. The source share, NFS
 * export state, and node mounts are never modified by this RPC.
 *
 * Backend:
 *   - Deletes through DELETE /api/v2/snapshot/{id}.
 *   - Uses the canonical snapshot id and typed NotFound classification.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires snapshot_id.
 *
 * @return *csi.DeleteSnapshotResponse: Empty response after deletion or
 *         idempotent absence.
 * @return error: InvalidArgument for a missing ID, Unavailable for temporary
 *         backend failure, and Internal for non-idempotent backend failure.
 */
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	d.log().Info("DeleteSnapshot(): init")
	d.log().Debug("DeleteSnapshot(): request",
		zap.Bool("request_present", req != nil),
		zap.String("snapshot_id", req.GetSnapshotId()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Validate identity before issuing the backend delete.
	if err := ctx.Err(); err != nil {
		d.log().Error("DeleteSnapshot(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetSnapshotId() == "" {
		d.log().Error("DeleteSnapshot(): snapshot ID is required")
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}
	snapshotID := req.GetSnapshotId()
	d.log().Info("DeleteSnapshot(): request accepted", zap.String("snapshot_id", snapshotID))
	if d.coordination == nil {
		return nil, status.Errorf(codes.Unavailable, "%s: coordinator is unavailable", "acquire NFS delete-snapshot lock")
	}
	lock, err := d.coordination.Acquire(ctx, "snapshot:"+snapshotID, coord.AcquireOptions{
		Safety: coord.SafetyDestructive, Operation: coord.OperationSnapshot,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "acquire NFS delete-snapshot lock", err)
	}
	var result *csi.DeleteSnapshotResponse
	criticalErr := lock.AssertOwned(ctx)
	if criticalErr != nil {
		if errors.Is(criticalErr, context.Canceled) || errors.Is(criticalErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(criticalErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS delete-snapshot lock ownership", criticalErr)
		}
		goto release
	}
	{
		if err := lock.AssertOwned(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				criticalErr = status.FromContextError(err).Err()
				goto release
			}
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify NFS delete-snapshot lock ownership", err)
			goto release
		}

		// Treat typed NotFound as CSI idempotent success; all other backend
		// failures remain visible to the CO. The SDK already folds an absent
		// snapshot into a nil error, so the typed branch guards races.
		if err := d.client.Snapshots().Delete(ctx, snapshotID); err != nil {
			if utils.ClassifyError(err) == utils.ErrorNotFound {
				d.log().Info("DeleteSnapshot(): snapshot is already absent",
					zap.String("snapshot_id", snapshotID),
				)
				result = &csi.DeleteSnapshotResponse{}
				goto owned
			}
			d.log().Error("DeleteSnapshot(): backend delete failed",
				zap.String("snapshot_id", snapshotID),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("delete snapshot", err)
			goto release
		}
		d.log().Info("DeleteSnapshot(): snapshot deleted", zap.String("snapshot_id", snapshotID))
		result = &csi.DeleteSnapshotResponse{}
		goto owned
	}
owned:
	if ownedErr := lock.AssertOwned(ctx); ownedErr != nil {
		if errors.Is(ownedErr, context.Canceled) || errors.Is(ownedErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(ownedErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS delete-snapshot lock ownership", ownedErr)
		}
	}
release:
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), coordinationReleaseTimeout)
	releaseErr := lock.Release(releaseCtx)
	releaseCancel()
	if criticalErr != nil {
		if releaseErr != nil {
			if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
				return nil, errors.Join(criticalErr, status.FromContextError(releaseErr).Err())
			}
			return nil, errors.Join(criticalErr, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS delete-snapshot lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(releaseErr).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS delete-snapshot lock", releaseErr)
	}
	return result, nil
}

/*
 * ListSnapshots returns canonical NGX snapshots with CSI filtering and
 * pagination. It never probes compatibility aliases for identity or time.
 * Tokens are stable canonical snapshot IDs prefixed with s1:.
 *
 * Backend:
 *   - Reads GET /api/v2/snapshot/list.
 *   - Uses canonical id, volume_id, created, and reserved fields.
 *   - Applies optional snapshot_id and source_volume_id filters in the driver.
 * created is an epoch string and reserved is a byte value.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Accepts optional filters, max_entries >= 0, and starting_token.
 *             An omitted request is treated as an empty request.
 *
 * @return *csi.ListSnapshotsResponse: Returns sorted snapshot entries and a
 *         next_token when more matching records remain.
 * @return error: InvalidArgument for negative max_entries, Aborted for an
 *         invalid token, Unavailable for backend failure, and Internal for
 *         malformed canonical snapshot data.
 */
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	d.log().Info("ListSnapshots(): init")
	d.log().Debug("ListSnapshots(): request",
		zap.Bool("request_present", req != nil),
		zap.String("snapshot_id", req.GetSnapshotId()),
		zap.String("source_volume_id", req.GetSourceVolumeId()),
		zap.Int32("max_entries", req.GetMaxEntries()),
		zap.String("starting_token", req.GetStartingToken()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Normalize an omitted request and reject malformed pagination before the
	// backend list operation.
	if err := ctx.Err(); err != nil {
		d.log().Error("ListSnapshots(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil {
		req = &csi.ListSnapshotsRequest{}
	}
	if req.GetMaxEntries() < 0 {
		d.log().Error("ListSnapshots(): max entries cannot be negative",
			zap.Int32("max_entries", req.GetMaxEntries()),
		)
		return nil, status.Error(codes.InvalidArgument, "max entries cannot be negative")
	}
	d.log().Info("ListSnapshots(): request accepted",
		zap.String("snapshot_id", req.GetSnapshotId()),
		zap.String("source_volume_id", req.GetSourceVolumeId()),
		zap.Int32("max_entries", req.GetMaxEntries()),
	)

	// A source volume that cannot be a real NGX volume (the backend rejects
	// non-canonical filters with HTTP 400) must yield an empty list, matching
	// the CSI contract for a nonexistent source volume.
	if req.GetSourceVolumeId() != "" && !ngxstorage.IsCanonicalVolumeID(req.GetSourceVolumeId()) {
		d.log().Info("ListSnapshots(): source volume id is not canonical, returning empty list",
			zap.String("source_volume_id", req.GetSourceVolumeId()),
		)
		return &csi.ListSnapshotsResponse{Entries: []*csi.ListSnapshotsResponse_Entry{}}, nil
	}

	// The API v2 detail endpoint is the authoritative snapshots view. CSI
	// supports filtering by either snapshot ID or source volume ID, so apply
	// those filters here instead of relying on the backend query endpoint.
	snapshots, err := d.client.Snapshots().ListDetail(ctx, req.GetSourceVolumeId())
	if err != nil {
		d.log().Error("ListSnapshots(): backend snapshot list failed", zap.Error(err))
		return nil, utils.BackendStatusError("list snapshots", err)
	}

	// Apply CSI filters to canonical records and reject malformed matching
	// snapshots instead of returning partial entries.
	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(snapshots))
	for i := range snapshots {
		snapshot := snapshots[i]
		snapshotID := snapshot.ID
		sourceVolumeID := snapshot.VolumeID
		if req.GetSnapshotId() != "" && snapshotID != req.GetSnapshotId() {
			continue
		}
		if req.GetSourceVolumeId() != "" && sourceVolumeID != req.GetSourceVolumeId() {
			continue
		}
		// Convert canonical fields here so list filtering, conversion failures,
		// and returned CSI status remain visible in this RPC.
		created := snapshot.Created
		reserved := snapshot.Size
		if snapshotID == "" {
			d.log().Error("ListSnapshots(): matching snapshot is missing id")
			return nil, status.Error(codes.Internal, "list snapshots: backend record missing id")
		}
		if sourceVolumeID == "" {
			d.log().Error("ListSnapshots(): snapshot is missing volume_id",
				zap.String("snapshot_id", snapshotID),
			)
			return nil, status.Error(codes.Internal, "list snapshots: backend record missing volume_id")
		}
		creationTime := utils.BackendCreatedTimestamp(created)
		if creationTime == nil {
			d.log().Error("ListSnapshots(): snapshot has invalid created",
				zap.String("snapshot_id", snapshotID),
				zap.String("created", created),
			)
			return nil, status.Error(codes.Internal, "list snapshots: backend record has invalid created")
		}
		sizeBytes, err := strconv.ParseInt(strings.TrimSpace(reserved), 10, 64)
		if err != nil || sizeBytes < 0 {
			d.log().Error("ListSnapshots(): snapshot has invalid reserved",
				zap.String("snapshot_id", snapshotID),
				zap.String("reserved", reserved),
				zap.Error(err),
			)
			return nil, status.Error(codes.Internal, "list snapshots: backend record has invalid reserved")
		}
		entrySnapshot := &csi.Snapshot{
			SnapshotId:     snapshotID,
			SourceVolumeId: sourceVolumeID,
			SizeBytes:      sizeBytes,
			CreationTime:   creationTime,
			ReadyToUse:     true,
		}
		d.log().Debug("ListSnapshots(): canonical snapshot converted",
			zap.String("snapshot_id", snapshotID),
			zap.String("source_volume_id", sourceVolumeID),
			zap.Int64("size_bytes", sizeBytes),
		)
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: entrySnapshot})
	}

	// Sort by canonical snapshot ID so pagination tokens are deterministic.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].GetSnapshot().GetSnapshotId() < entries[j].GetSnapshot().GetSnapshotId()
	})
	start := 0
	const tokenPrefix = "s1:"
	if token := req.GetStartingToken(); token != "" {
		if !strings.HasPrefix(token, tokenPrefix) || len(token) == len(tokenPrefix) {
			d.log().Error("ListSnapshots(): invalid starting token")
			return nil, status.Error(codes.Aborted, "invalid starting token")
		}
		cursor := strings.TrimPrefix(token, tokenPrefix)
		for start < len(entries) && entries[start].GetSnapshot().GetSnapshotId() <= cursor {
			start++
		}
	}
	end := len(entries)
	if maxEntries := req.GetMaxEntries(); maxEntries > 0 && int64(start)+int64(maxEntries) < int64(end) {
		end = start + int(maxEntries)
	}
	// Slice the sorted result and derive the next cursor from the final returned
	// snapshot ID.
	response := &csi.ListSnapshotsResponse{Entries: entries[start:end]}
	if end < len(entries) {
		response.NextToken = tokenPrefix + entries[end-1].GetSnapshot().GetSnapshotId()
	}
	d.log().Info("ListSnapshots(): snapshots returned",
		zap.Reflect("entries", response.Entries),
		zap.String("next_token", response.NextToken),
	)
	d.log().Debug("ListSnapshots(): response",
		zap.Reflect("entries", response.Entries),
		zap.String("next_token", response.NextToken),
	)
	return response, nil
}

/*
 * ControllerExpandVolume increases an NGX NFS share soft quota.
 * CapacityRange and capacity.soft_quota are bytes. Equal capacity is an
 * idempotent success; shrinking is rejected. NFS expansion does not rescan a
 * node-local storage operation.
 *
 * Backend:
 *   - Reads GET /api/v2/share/{id}.
 *   - Writes PATCH /api/v2/share/{id} with soft_quota.
 *   - Uses canonical capacity.soft_quota.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id and capacity_range. required_bytes must be
 *             positive and must not be smaller than the current quota.
 *
 * @return *csi.ControllerExpandVolumeResponse: Returns capacity_bytes and
 *         node_expansion_required=false.
 * @return error: InvalidArgument for missing fields or shrink requests,
 *         NotFound for an absent share, Unavailable for temporary backend
 *         failure, and Internal for malformed quota data.
 */
func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	d.log().Info("ControllerExpandVolume(): init")
	d.log().Debug("ControllerExpandVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
		zap.Reflect("capacity_range", req.GetCapacityRange()),
		zap.Reflect("volume_capability", req.GetVolumeCapability()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	// Validate identity and byte capacity before reading current backend quota.
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerExpandVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("ControllerExpandVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetCapacityRange() == nil {
		d.log().Error("ControllerExpandVolume(): capacity range is required",
			zap.String("volume_id", req.GetVolumeId()),
		)
		return nil, status.Error(codes.InvalidArgument, "capacity range is required")
	}

	requiredBytes := req.GetCapacityRange().GetRequiredBytes()
	if requiredBytes <= 0 {
		d.log().Error("ControllerExpandVolume(): required capacity must be positive",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Int64("required_bytes", requiredBytes),
		)
		return nil, status.Error(codes.InvalidArgument, "required capacity must be positive")
	}
	if d.coordination == nil {
		return nil, status.Errorf(codes.Unavailable, "%s: coordinator is unavailable", "acquire NFS expand-volume lock")
	}
	lock, err := d.coordination.Acquire(ctx, "volume:"+req.GetVolumeId(), coord.AcquireOptions{
		Safety: coord.SafetyOrdinary, Operation: coord.OperationExpand,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "acquire NFS expand-volume lock", err)
	}
	var result *csi.ControllerExpandVolumeResponse
	criticalErr := lock.AssertOwned(ctx)
	if criticalErr != nil {
		if errors.Is(criticalErr, context.Canceled) || errors.Is(criticalErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(criticalErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS expand-volume lock ownership", criticalErr)
		}
		goto release
	}
	{
		// Read current soft_quota from the canonical share detail so equal requests
		// are idempotent and shrink requests are rejected before mutation.
		current, err := d.client.Shares().Get(ctx, req.GetVolumeId())
		if err != nil {
			d.log().Error("ControllerExpandVolume(): backend lookup failed",
				zap.String("volume_id", req.GetVolumeId()),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("expand volume lookup", err)
			goto release
		}
		currentID := current.ID
		if currentID == "" || currentID != req.GetVolumeId() {
			d.log().Error("ControllerExpandVolume(): backend response has invalid id",
				zap.String("requested_volume_id", req.GetVolumeId()),
				zap.String("backend_volume_id", currentID),
			)
			criticalErr = status.Error(codes.Internal, "expand volume: backend response has an invalid id")
			goto release
		}
		currentBytes := helper.ShareSoftQuotaBytes(current)
		if currentBytes == 0 {
			d.log().Error("ControllerExpandVolume(): backend response has invalid soft quota",
				zap.String("volume_id", req.GetVolumeId()),
			)
			criticalErr = status.Error(codes.Internal, "expand volume: backend response missing soft_quota")
			goto release
		}
		if requiredBytes < currentBytes {
			d.log().Warn("ControllerExpandVolume(): volume shrink is not supported",
				zap.String("volume_id", req.GetVolumeId()),
				zap.Int64("current_bytes", currentBytes),
				zap.Int64("required_bytes", requiredBytes),
			)
			criticalErr = status.Error(codes.InvalidArgument, "requested capacity must not shrink the volume")
			goto release
		}
		if requiredBytes == currentBytes {
			d.log().Info("ControllerExpandVolume(): volume already has requested capacity",
				zap.String("volume_id", req.GetVolumeId()),
				zap.Int64("capacity_bytes", currentBytes),
			)
			result = &csi.ControllerExpandVolumeResponse{CapacityBytes: currentBytes}
			goto owned
		}
		if err := lock.AssertOwned(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				criticalErr = status.FromContextError(err).Err()
				goto release
			}
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify NFS expand-volume lock ownership", err)
			goto release
		}
		// PATCH only soft_quota in bytes. NFS requires no node filesystem resize.
		if err := d.client.Shares().Expand(ctx, req.GetVolumeId(), requiredBytes); err != nil {
			d.log().Error("ControllerExpandVolume(): backend quota update failed",
				zap.String("volume_id", req.GetVolumeId()),
				zap.Int64("required_bytes", requiredBytes),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("expand volume", err)
			goto release
		}
		d.log().Info("ControllerExpandVolume(): volume expanded",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Int64("previous_bytes", currentBytes),
			zap.Int64("capacity_bytes", requiredBytes),
		)
		result = &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         requiredBytes,
			NodeExpansionRequired: false,
		}
		goto owned
	}
owned:
	if ownedErr := lock.AssertOwned(ctx); ownedErr != nil {
		if errors.Is(ownedErr, context.Canceled) || errors.Is(ownedErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(ownedErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS expand-volume lock ownership", ownedErr)
		}
	}
release:
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), coordinationReleaseTimeout)
	releaseErr := lock.Release(releaseCtx)
	releaseCancel()
	if criticalErr != nil {
		if releaseErr != nil {
			if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
				return nil, errors.Join(criticalErr, status.FromContextError(releaseErr).Err())
			}
			return nil, errors.Join(criticalErr, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS expand-volume lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(releaseErr).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS expand-volume lock", releaseErr)
	}
	return result, nil
}

/*
 * ControllerGetVolume returns the current canonical NGX share identity and
 * quota. It is read-only and does not depend on process-local share state.
 *
 * Backend:
 *   - Reads GET /api/v2/share/{id}.
 *   - Uses canonical id and capacity.soft_quota in bytes.
 * The driver does not advertise LIST_VOLUMES_PUBLISHED_NODES or
 * VOLUME_CONDITION, so the required status object is returned without guessed
 * node or condition data.
 *
 * @param ctx: Carries the CSI deadline and cancellation signal.
 * @param req: Requires volume_id.
 *
 * @return *csi.ControllerGetVolumeResponse: Returns the canonical CSI volume
 *         and an empty required volume status object.
 * @return error: InvalidArgument for a missing ID, NotFound for an absent
 *         share, Unavailable for temporary backend failure, and Internal for
 *         malformed identity or quota data.
 */
func (d *Driver) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	d.log().Info("ControllerGetVolume(): init")
	d.log().Debug("ControllerGetVolume(): request",
		zap.Bool("request_present", req != nil),
		zap.String("volume_id", req.GetVolumeId()),
	)

	// Validate the read-only lookup before issuing a backend request.
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerGetVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("ControllerGetVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Read canonical identity and quota directly from NGX; local cache state is
	// not authoritative after controller restart.
	share, err := d.client.Shares().Get(ctx, req.GetVolumeId())
	if err != nil {
		d.log().Error("ControllerGetVolume(): backend lookup failed",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Error(err),
		)
		return nil, utils.BackendStatusError("get volume", err)
	}
	volumeID := share.ID
	if volumeID == "" || volumeID != req.GetVolumeId() {
		d.log().Error("ControllerGetVolume(): backend response has invalid id",
			zap.String("requested_volume_id", req.GetVolumeId()),
			zap.String("backend_volume_id", volumeID),
		)
		return nil, status.Error(codes.Internal, "get volume: backend response has an invalid id")
	}
	capacityBytes := helper.ShareSoftQuotaBytes(share)
	if capacityBytes <= 0 {
		d.log().Error("ControllerGetVolume(): backend response has invalid soft quota",
			zap.String("volume_id", volumeID),
		)
		return nil, status.Error(codes.Internal, "get volume: backend response has invalid soft_quota")
	}
	// Return the required CSI status object without inventing published nodes
	// or conditions that this driver does not advertise.
	response := &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: capacityBytes,
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{},
	}
	d.log().Info("ControllerGetVolume(): volume returned",
		zap.String("volume_id", volumeID),
		zap.Int64("capacity_bytes", capacityBytes),
	)
	d.log().Debug("ControllerGetVolume(): response", zap.Reflect("response", response))
	return response, nil
}

/*
 * ControllerModifyVolume applies one explicit set of supported mutable NGX
 * share attributes. It is the CSI VolumeAttributesClass update path; omitted
 * fields are never defaulted or changed. Immutable create parameters such as
 * blocksize, quota, export state, and mount options are rejected.
 *
 * Backend:
 *   - Reads GET /api/v2/share/{id} to resolve the canonical share.
 *   - Writes PATCH /api/v2/share/{id} with only the validated mutable fields.
 *   - Accepts compression, deduplication, DRAM cache, flash cache,
 *     permissions, and flashtier_limit.
 *
 * @param ctx: Carries the CSI deadline through lookup and mutation.
 * @param req: Requires volume_id; supplied mutable parameters must use the
 *             supported keys and values are normalized to the NGX PATCH schema.
 *
 * @return *csi.ControllerModifyVolumeResponse: Empty success response after
 *         the backend accepts the exact requested update.
 * @return error: InvalidArgument for missing IDs or unsupported parameters,
 *         NotFound for an absent volume, Unavailable for backend transport or
 *         pool placement failure, and the context status on cancellation.
 */
func (d *Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	// Normalize the request and validate cancellation before any mutation.
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerModifyVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("ControllerModifyVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	mutable, err := helper.NormalizeMutableParameters(req.GetMutableParameters())
	if err != nil {
		d.log().Error("ControllerModifyVolume(): mutable parameters rejected",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Error(err),
		)
		return nil, err
	}
	if d.coordination == nil {
		return nil, status.Errorf(codes.Unavailable, "%s: coordinator is unavailable", "acquire NFS modify-volume lock")
	}
	lock, err := d.coordination.Acquire(ctx, "volume:"+req.GetVolumeId(), coord.AcquireOptions{
		Safety: coord.SafetyOrdinary, Operation: coord.OperationModify,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "acquire NFS modify-volume lock", err)
	}
	var result *csi.ControllerModifyVolumeResponse
	criticalErr := lock.AssertOwned(ctx)
	if criticalErr != nil {
		if errors.Is(criticalErr, context.Canceled) || errors.Is(criticalErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(criticalErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS modify-volume lock ownership", criticalErr)
		}
		goto release
	}
	{

		// Resolve the volume against the backend so a missing share returns the
		// CSI-compliant NotFound instead of a backend-style 404.
		share, err := d.client.Shares().Get(ctx, req.GetVolumeId())
		if err != nil {
			d.log().Error("ControllerModifyVolume(): share lookup failed",
				zap.String("volume_id", req.GetVolumeId()),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("lookup volume", err)
			goto release
		}
		if share == nil || share.ID == "" || share.ID != req.GetVolumeId() {
			d.log().Error("ControllerModifyVolume(): volume not found",
				zap.String("volume_id", req.GetVolumeId()),
			)
			criticalErr = status.Error(codes.NotFound, "volume not found")
			goto release
		}
		if err := lock.AssertOwned(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				criticalErr = status.FromContextError(err).Err()
				goto release
			}
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify NFS modify-volume lock ownership", err)
			goto release
		}

		if err := d.reconciler().ModifyShareOnOwner(ctx, req.GetVolumeId(), mutable); err != nil {
			d.log().Error("ControllerModifyVolume(): backend modification failed",
				zap.String("volume_id", req.GetVolumeId()),
				zap.Error(err),
			)
			criticalErr = utils.BackendStatusError("modify share", err)
			goto release
		}
		d.log().Info("ControllerModifyVolume(): volume modified",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Reflect("mutable_parameters", mutable),
		)
		result = &csi.ControllerModifyVolumeResponse{}
		goto owned
	}
owned:
	if ownedErr := lock.AssertOwned(ctx); ownedErr != nil {
		if errors.Is(ownedErr, context.Canceled) || errors.Is(ownedErr, context.DeadlineExceeded) {
			criticalErr = status.FromContextError(ownedErr).Err()
		} else {
			criticalErr = status.Errorf(codes.Unavailable, "%s: %v", "verify acquire NFS modify-volume lock ownership", ownedErr)
		}
	}
release:
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), coordinationReleaseTimeout)
	releaseErr := lock.Release(releaseCtx)
	releaseCancel()
	if criticalErr != nil {
		if releaseErr != nil {
			if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
				return nil, errors.Join(criticalErr, status.FromContextError(releaseErr).Err())
			}
			return nil, errors.Join(criticalErr, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS modify-volume lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		if errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded) {
			return nil, status.FromContextError(releaseErr).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "%s: %v", "release acquire NFS modify-volume lock", releaseErr)
	}
	return result, nil
}

/*
 * ControllerGetVolumeHealth confirms that one canonical NGX share is
 * reachable through the controller. A resolved share is reported with an
 * empty health-status list, which is CSI's healthy representation; this
 * driver does not invent degraded conditions absent from the backend contract.
 *
 * Backend:
 *   - Reads GET /api/v2/share/{id}.
 *   - Uses only the canonical share ID to confirm identity.
 *   - Performs no export, quota, mount, or other backend mutation.
 *
 * @param ctx: Carries the CSI deadline through the backend read.
 * @param req: Requires a canonical dynamic volume_id.
 *
 * @return *csi.ControllerGetVolumeHealthResponse: The volume ID and its
 *         current CSI health-status list.
 * @return error: InvalidArgument for a missing ID, NotFound for an absent or
 *         mismatched share, Unavailable for backend transport or placement
 *         failure, and the context status on cancellation.
 */
func (d *Driver) ControllerGetVolumeHealth(ctx context.Context, req *csi.ControllerGetVolumeHealthRequest) (*csi.ControllerGetVolumeHealthResponse, error) {
	d.log().Info("ControllerGetVolumeHealth(): init")
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerGetVolumeHealth(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("ControllerGetVolumeHealth(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	share, err := d.client.Shares().Get(ctx, req.GetVolumeId())
	if err != nil {
		d.log().Error("ControllerGetVolumeHealth(): share lookup failed",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Error(err),
		)
		return nil, utils.BackendStatusError("lookup volume", err)
	}
	if share == nil || share.ID != req.GetVolumeId() {
		d.log().Error("ControllerGetVolumeHealth(): volume not found",
			zap.String("volume_id", req.GetVolumeId()),
		)
		return nil, status.Error(codes.NotFound, "volume not found")
	}
	d.log().Debug("ControllerGetVolumeHealth(): share resolved", zap.String("volume_id", req.GetVolumeId()))
	return &csi.ControllerGetVolumeHealthResponse{VolumeHealth: &csi.VolumeHealth{
		VolumeId:       req.GetVolumeId(),
		HealthStatuses: helper.HealthyStatuses(share),
	}}, nil
}

/*
 * ControllerListVolumeHealth returns health records for every canonical NGX
 * share. Each resolved share is healthy with an empty status list; malformed
 * list entries without an ID are skipped rather than being exposed as invalid
 * CSI records. This RPC is read-only and intentionally has no pagination
 * because the CSI v1.13 request does not define a token.
 *
 * Backend:
 *   - Reads GET /api/v2/share/list.
 *   - Uses canonical share IDs only; no process-local cache is authoritative.
 *   - Performs no export, quota, snapshot, or mount mutation.
 *
 * @param ctx: Carries the CSI deadline through the backend list request.
 * @param req: Empty CSI request; no filters are supported by this RPC.
 *
 * @return *csi.ControllerListVolumeHealthResponse: One health entry for each
 *         well-formed backend share.
 * @return error: Unavailable for backend transport or placement failure and
 *         the context status on cancellation.
 */
func (d *Driver) ControllerListVolumeHealth(ctx context.Context, req *csi.ControllerListVolumeHealthRequest) (*csi.ControllerListVolumeHealthResponse, error) {
	d.log().Info("ControllerListVolumeHealth(): init")
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerListVolumeHealth(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	shares, err := d.client.Shares().List(ctx)
	if err != nil {
		d.log().Error("ControllerListVolumeHealth(): backend share list failed", zap.Error(err))
		return nil, utils.BackendStatusError("list volumes", err)
	}
	entries := make([]*csi.VolumeHealth, 0, len(shares))
	for i := range shares {
		share := shares[i]
		if share.ID == "" {
			continue
		}
		entries = append(entries, &csi.VolumeHealth{
			VolumeId:       share.ID,
			HealthStatuses: helper.HealthyStatuses(&share),
		})
	}
	d.log().Debug("ControllerListVolumeHealth(): entries resolved", zap.Int("count", len(entries)))
	return &csi.ControllerListVolumeHealthResponse{Entries: entries}, nil
}

// ---------------------------------------------------------------------------
// Controller share create/name-lookup/canonical-5011 owner-modify
// ---------------------------------------------------------------------------
