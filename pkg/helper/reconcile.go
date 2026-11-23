package helper

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	utils "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/utils"
	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	zap "go.uber.org/zap"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const (
	NFSDeleteVerifyTimeout  = 60 * time.Second
	NFSDeleteVerifyInterval = 2 * time.Second
)

// Reconciler owns NFS backend share reconcile/verification orchestration.
// The CSI mutation-replay policy stays here; the SDK owns transport only.
type Reconciler struct {
	Client               *ngxstorage.Client
	DeleteVerifyTimeout  time.Duration
	DeleteVerifyInterval time.Duration
	Log                  *zap.Logger
}

func (r *Reconciler) log() *zap.Logger {
	if r.Log != nil {
		return r.Log
	}
	return zap.NewNop()
}

// verifyShareDeleted polls the backend until the share is authoritatively
// NotFound, bounded by the request context and NFSDeleteVerifyTimeout.
//   - present share: keep polling within budget; budget exhausted -> error
//     (CSI success must not be claimed while the backend object exists).
//   - ambiguous GET error (403/timeout/5xx): retry within budget; NEVER
//     treated as absence; budget exhausted -> error.
//   - authoritative NotFound: success (idempotent).
//
// Lease/ownership rules are enforced by the caller; this method only reads.
func (r *Reconciler) VerifyShareDeleted(ctx context.Context, volumeID string) error {
	timeout := r.DeleteVerifyTimeout
	if timeout <= 0 {
		timeout = NFSDeleteVerifyTimeout
	}
	interval := r.DeleteVerifyInterval
	if interval <= 0 {
		interval = NFSDeleteVerifyInterval
	}
	vctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		_, err := r.Client.Shares().Get(vctx, volumeID)
		switch {
		case err == nil:
			lastErr = status.Errorf(codes.Internal,
				"delete verification: share %s still present after a successful delete response (backend acknowledged but did not delete)", volumeID)
		case shareAbsenceProven(err):
			return nil
		default:
			lastErr = status.Errorf(codes.Internal,
				"delete verification: share %s absence GET ambiguous (not absence proof): %v", volumeID, err)
		}
		select {
		case <-vctx.Done():
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			return status.Errorf(codes.DeadlineExceeded,
				"delete verification budget exhausted for share %s; absence NOT proven: %v", volumeID, lastErr)
		case <-time.After(interval):
		}
	}
}

// shareAbsenceProven reports whether err is an AUTHORITATIVE backend absence
// signal for the share: the canonical NotFound code (5011) with HTTP 404, or
// a code-less HTTP 404 from the share endpoint. It is deliberately stricter
// than the general ClassifyError mapping (which treats any statusCode==404
// or code==5011 as NotFound for CO error reporting): a bare HTTP 404 carrying
// a DIFFERENT explicit API code is not absence proof, and a non-404 error
// status carrying code 5011 (e.g. a gateway 403/500) must never be read as a
// successful delete. No other HTTP error status or transport/discovery
// failure proves absence; those stay ambiguous and fail closed.
func shareAbsenceProven(err error) bool {
	var apiErr *ngxstorage.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.StatusCode != 404 {
		return false
	}
	return apiErr.Code == ngxstorage.CodeNotFound || apiErr.Code == 0
}

// createShareReconciled creates a name-keyed share and survives a lost
// response: the backend enforces unique names, so a re-list by name resolves
// the outcome before any retry.
func (r *Reconciler) CreateShareReconciled(ctx context.Context, req ngxstorage.ShareCreateRequest) (*ngxstorage.Share, error) {
	var lastErr error
	for attempt := 1; attempt <= utils.ReconcileAttempts; attempt++ {
		share, err := r.Client.Shares().Create(ctx, req)
		if err == nil {
			return share, nil
		}
		lastErr = err
		if !ngxstorage.IsTransportError(err) {
			return nil, err
		}
		if existing, found, lerr := r.ShareByName(ctx, req.Name); lerr == nil && found {
			r.log().Warn("share create reconciled after transport failure",
				zap.String("name", req.Name), zap.String("share_id", existing.ID))
			return existing, nil
		}
		if serr := utils.ReconcileBackoff(ctx, attempt); serr != nil {
			return nil, serr
		}
	}
	return nil, lastErr
}

// shareByName returns the share with the exact name.
func (r *Reconciler) ShareByName(ctx context.Context, name string) (*ngxstorage.Share, bool, error) {
	shares, err := r.Client.Shares().List(ctx)
	if err != nil {
		return nil, false, err
	}
	for i := range shares {
		if shares[i].Name == name {
			return &shares[i], true, nil
		}
	}
	return nil, false, nil
}

// modifyShareOnOwner applies a share mutation and recovers from a clustered
// array's controller-local view: only a canonical NGX 5011 is treated as
// "possibly the stale controller". The owner is re-resolved and the mutation
// is retried exactly once, and only when the selection actually changed. A
// code-less HTTP 404 or a transport failure is returned unchanged — the SDK
// request layer never replays a write.
func (r *Reconciler) ModifyShareOnOwner(ctx context.Context, shareID string, fields map[string]interface{}) error {
	err := r.Client.Shares().Modify(ctx, shareID, fields)
	if err == nil || !ngxstorage.IsCanonicalNotFoundError(err) {
		return err
	}
	before := r.Client.CurrentController()
	if refreshErr := r.Client.RefreshController(ctx); refreshErr != nil || r.Client.CurrentController() == before {
		return err
	}
	r.log().Warn("share modify retried on the pool-owning controller",
		zap.String("share_id", shareID),
		zap.String("controller", r.Client.CurrentController()),
	)
	return r.Client.Shares().Modify(ctx, shareID, fields)
}

// SnapshotByID looks up one snapshot from the backend list and prepares its
// CSI proto. A nil snapshot with nil error means absence; the RPC owner makes
// the NotFound decision. Backend errors return unwrapped for classification.
func (r *Reconciler) SnapshotByID(ctx context.Context, snapshotID string) (*csi.Snapshot, error) {
	snapshots, err := r.Client.Snapshots().ListDetail(ctx, "")
	if err != nil {
		return nil, err
	}
	for i := range snapshots {
		snapshot := snapshots[i]
		if snapshot.ID != snapshotID {
			continue
		}
		// Mirror the ListSnapshots/CreateSnapshot record validation: a
		// malformed canonical snapshot record is an Internal error, and the
		// returned snapshot carries the same SizeBytes/CreationTime surface.
		if snapshot.ID == "" {
			return nil, status.Error(codes.Internal, "get snapshot: backend record missing id")
		}
		if snapshot.VolumeID == "" {
			return nil, status.Error(codes.Internal, "get snapshot: backend record missing volume_id")
		}
		creationTime := utils.BackendCreatedTimestamp(snapshot.Created)
		if creationTime == nil {
			return nil, status.Error(codes.Internal, "get snapshot: backend record has invalid created")
		}
		sizeBytes, err := strconv.ParseInt(strings.TrimSpace(snapshot.Size), 10, 64)
		if err != nil || sizeBytes < 0 {
			return nil, status.Error(codes.Internal, "get snapshot: backend record has invalid reserved")
		}
		return &csi.Snapshot{
			SnapshotId:     snapshot.ID,
			SourceVolumeId: snapshot.VolumeID,
			SizeBytes:      sizeBytes,
			CreationTime:   creationTime,
			ReadyToUse:     true,
		}, nil
	}
	return nil, nil
}

// ResolvePublishShare resolves the backend share for a publish: static
// handles by administrator-provided share name, dynamic handles by canonical
// backend ID. Lookup failures keep their gRPC classification.
func (r *Reconciler) ResolvePublishShare(ctx context.Context, volumeID string, staticVolume bool) (*ngxstorage.Share, error) {
	var (
		share     *ngxstorage.Share
		backendID string
	)
	// Static handles resolve by administrator-provided share name. Dynamic
	// handles resolve directly by canonical backend ID.
	if staticVolume {
		shareName := ShareName(volumeID)
		shares, err := r.Client.Shares().List(ctx)
		if err != nil {
			r.log().Error("ControllerPublishVolume(): static share list failed",
				zap.String("volume_id", volumeID),
				zap.String("share_name", shareName),
				zap.Error(err),
			)
			return nil, utils.BackendStatusError("publish volume lookup", err)
		}
		for i := range shares {
			if shares[i].Name == shareName {
				share = &shares[i]
				break
			}
		}
		if share == nil {
			r.log().Error("ControllerPublishVolume(): static share was not found",
				zap.String("volume_id", volumeID),
				zap.String("share_name", shareName),
			)
			return nil, status.Errorf(codes.NotFound, "volume %s was not found", volumeID)
		}
		backendID = share.ID
	} else {
		var err error
		share, err = r.Client.Shares().Get(ctx, volumeID)
		if err != nil {
			r.log().Error("ControllerPublishVolume(): backend lookup failed",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
			return nil, utils.BackendStatusError("publish volume lookup", err)
		}
		backendID = share.ID
	}
	if backendID == "" || (!staticVolume && backendID != volumeID) {
		r.log().Error("ControllerPublishVolume(): backend response has invalid id",
			zap.String("volume_id", volumeID),
			zap.String("backend_id", backendID),
		)
		return nil, status.Error(codes.Internal, "publish volume: backend response has an invalid id")
	}
	return share, nil
}
