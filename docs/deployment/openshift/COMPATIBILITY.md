# OpenShift compatibility

## Reviewed baseline

| Property | Value |
| --- | --- |
| Platform | Red Hat OpenShift Container Platform 4.20.22 |
| Kubernetes payload | 1.33.10 (validated API server patch: 1.33.11) |
| Architecture | Linux amd64 |
| Release image | Resolve from the target cluster with `oc get clusterversion version` |

The release payload is the authority for sidecar images. A different z-stream
or architecture requires regenerating and reviewing the complete sidecar set.

## Supported architecture

Supported architecture for the initial certified release:

```text
AMD64 / x86_64 (linux/amd64)
```

The Operator manager, controller operand, and node DaemonSet always carry the
mandatory platform constraints `kubernetes.io/os=linux` and
`kubernetes.io/arch=amd64`. User placement selectors (`controllerNodeSelector`,
`nodeNodeSelector`) are **additive** to these constraints; a selector that
contradicts them (e.g. `os=windows` or `arch=arm64`) is rejected and the CR is
reported `Degraded` with reason `InvalidPlacement`.

This project does **not** claim multi-arch support for the initial release.
Future ARM64 support requires, at minimum:

```text
multi-arch driver image
multi-arch Operator image
sidecar compatibility (arm64 payload)
OCP ARM64 functional test
CSI functional certification on ARM64
```

## Sidecar set

The raw OpenShift manifests and OpenShift Helm chart use these reviewed OCP
payload pullspecs:

| Component | Version | Immutable pullspec |
| --- | --- | --- |
| external-provisioner | v5.3.0 | `quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:a64dccdc647ac1c0f0ca29b74e15b328a3f62f0a46d2a563c6e2841f9ecb01d1` |
| external-attacher | v4.9.0 | `quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:077da8b01b8d7e4d0f753e99fcf41d4a2091d6865c205f64c8fecec1575241d4` |
| external-resizer | v1.14.0 | `quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:24ad86c3b010d127686550e0cfa1cfe4ae9f4d64778cef6c244fa48d0617623d` |
| external-snapshotter | v8.3.0 | `quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:1b9ebae30a269e6511597904ff480515caad89dbe11477f3a251dec58f784c92` |
| node-driver-registrar | v2.14.0 | `quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:715fed4adf4280d9a8e92a09cdcc35ed9bb95e623b1d5fc2f1b753981ecf41f4` |
| livenessprobe | v2.16.0 | `quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:ccf6bdeaf2a070edacb7c24a4e46af27a65dc25faf0fb4edc37cdfa922109b67` |

Resolve the current payload before a release change:

```bash
release_image="$(oc get clusterversion version -o jsonpath='{.status.desired.image}')"
oc adm release info "$release_image"
for component in csi-external-provisioner csi-external-attacher \
  csi-external-resizer csi-external-snapshotter csi-node-driver-registrar \
  csi-livenessprobe; do
  oc adm release info "$release_image" --image-for="$component"
done
```

## Snapshot ownership

OpenShift owns the Snapshot Controller Operator, snapshot CRDs, and cluster
snapshot controller. This driver owns only its external-snapshotter sidecar,
RBAC permissions, and driver-specific `VolumeSnapshotClass`. It must not
install or remove platform snapshot components.

## CSI wire compatibility

The driver is compiled against CSI v1.13.0. The charts and manifests use the
latest upstream CSI sidecars (`registry.k8s.io/sig-storage/*`), which fully
support the CSI 1.13 capability set the driver advertises (GET_VOLUME,
GET_SNAPSHOT, GET_VOLUME_HEALTH, LIST_VOLUME_HEALTH, MODIFY_VOLUME, etc.).
Older sidecar builds (e.g. csi-provisioner v5.2.0) cannot range over these
newer capability enums and panic in their log sanitizer; do not downgrade
below the pinned upstream set.
