# Immutable image deployment and rollback

Deployment installs the driver from scratch: the Quartet Operator reconciles
the controller Deployment and node DaemonSet from the image digest pinned in
the Quartet CR. Images are built from reviewed, committed source and referenced
by immutable digest; release tags are never overwritten.

To change the deployed image, record the current CR, image digest, and active
StorageClasses, then set the new digest in the CR. The Operator updates the
workloads without deleting customer PVCs, PVs, or NGX Storage backend shares. Validate
controller and node readiness, then exercise a mounted PVC. A worker reboot is
not part of this procedure; if a change requires cordon/drain or has a known
mount disruption, that requirement belongs in its release notes.

Rollback means re-applying the recorded immutable digest in the CR. The
Operator does not roll back backend data and never deletes customer shares as
part of uninstall or rollback.

No compatibility or certification claim is made here; OpenShift and CSI
functional results must be attached to each release before publication.