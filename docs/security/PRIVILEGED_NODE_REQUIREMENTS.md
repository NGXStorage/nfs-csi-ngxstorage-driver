# Privileged node requirements for the NGX Storage NFS CSI driver

Status: **documentation for Red Hat review** — this is a requirement
justification, **not** an approved exemption.

This document explains exactly why the NGX Storage NFS CSI **node** component runs
with root and privileged host-level access. Red Hat's 2026 OpenShift
certification policy does not prohibit containers that need privileged host
integration **when the privilege is genuinely required for the component's
function** and is documented; such components are subject to manual review and
must be flagged as requiring privileged host-level access in the certification
project. ([Red Hat OpenShift Software Certification Policy Guide][policy])

The **Operator manager** is deliberately non-root and restricted; only the
**CSI node plugin** requires privileged host-level integration.

[policy]: https://docs.redhat.com/en/documentation/red_hat_openshift_software_certification/2026/html-single/red_hat_openshift_software_certification_policy_guide/index

## Scope: which component needs privilege

| Component | Privileged? | Why |
| --- | --- | --- |
| Operator manager (controller-runtime reconciler) | No | Reconciles CRs and Kubernetes objects via the API server only |
| CSI controller (provisioner/attacher/... sidecars + driver) | No | Handles control-plane RPCs; no host mount access |
| **CSI node plugin** | **Yes** | Performs host NFS mount/unmount on behalf of kubelet |

## Implementation requirements (current implementation)

The node DaemonSet runs privileged with the following:

| Requirement | Current implementation | Justification |
| --- | --- | --- |
| **Privileged container** | `securityContext.privileged: true` | Needed to perform mounts into the host mount namespace |
| **`SYS_ADMIN` capability** | added | Required for mount/unmount of NFS filesystems into the host |
| **`SYS_CHROOT` capability** | added | Required to chroot into `/host` to execute host utilities |
| **`/host` mount** (bidirectional) | hostPath `/` mounted at `/host` | The driver executes host mount/umount/findmnt/stat via the `chroot-host-wrapper.sh` into the host root, because CSI node plugins act on the host mount namespace |
| **`/var/lib/kubelet` mount** (bidirectional) | hostPath mount | Needed for the plugin registration dir, socket dir, and to reach volumes kubelet mounts |
| **`/dev` access** | hostPath mount | Legacy compatibility retained; device nodes are not required for NFS but retained pending regression testing |
| **`hostNetwork`** | true | Node plugins traditionally run in the host network namespace (registrar + socket endpoints); retained for compatibility |
| **`hostPID` / `hostIPC`** | true | Compatibility profile retained; not required for NFS mounts specifically, pending regression testing |
| **NFS mount/unmount on behalf of kubelet** | driver calls `mount`/`umount` in the host | This is the fundamental function of a CSI node plugin for a network filesystem |

## Required by the current implementation vs. required by CSI itself

Red Hat's CSI certification requires the plugin to perform volume lifecycle
operations (`NodeStageVolume`/`NodePublishVolume`/`NodeUnpublishVolume`/
`NodeUnstageVolume`) which for a filesystem-based driver means executing
`mount`/`umount` in the **host** mount namespace. The privilege needed is a
consequence of the CSI node plugin contract for NFS.

Some items above are **compatibility profile retained pending regression
testing**, not strictly mandated by NFS mounting itself:

- `/dev` hostPath
- `hostPID` / `hostIPC`
- `hostNetwork`

These are kept because the node plugin is designed to run with the same
footprint across the platform lifecycle, and removing them requires a
dedicated regression pass (which is out of scope for the initial certified
release). They are listed separately here so the reviewer can weigh them.

## What is NOT privileged

- The **Operator manager**:
  - `runAsNonRoot: true`
  - `readOnlyRootFilesystem: true`
  - drops ALL capabilities
  - `allowPrivilegeEscalation: false`
  - `seccompProfile: RuntimeDefault`
- The **CSI controller** containers run non-privileged with dropped
  capabilities.
- The SCC used by the node plugin has **no direct user/SA entries**
  (RBAC-only authorization: the node ServiceAccount gets `use` on the SCC via
  a dedicated ClusterRole/ClusterRoleBinding).

## Certification project action (required, external)

For Red Hat certification, the driver container component must be marked in
the Partner Connect certification project as:

```text
privileged host-level access required: YES
```

and the container must be submitted for the standard container
certification + `RunAsNonRoot` exception review. Red Hat reviews whether the
privilege is justified for a storage node plugin.

## Status

```text
Preflight check:  RunAsNonRoot
Result:           FAIL
Reason:           privileged host-level CSI node integration is required
Mitigation:       privilege isolated to CSI node workload; Operator manager is non-root
Red Hat project action: mark component as requiring privileged host-level access
Status:           PENDING RED HAT REVIEW
```

This is **not** an approved exemption. Do not claim approval until Red Hat
confirms it.
