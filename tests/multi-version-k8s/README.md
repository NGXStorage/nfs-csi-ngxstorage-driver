# Multi-version Kubernetes test (destructive legacy harness)

This historical harness validates the NGX Storage NFS CSI driver on Kubernetes
versions 1.24 through 1.36 using kubeadm. It resets the host cluster, removes
host Kubernetes state, changes installed Kubernetes packages, and creates
backend resources. It is excluded from `tests/run-all-tests.sh` and must only
be considered on an isolated disposable VM after a separate safety review.

## Prerequisites

- 1 control-plane node (disposable VM, Ubuntu 26.04)
- `ngx` user with passwordless sudo (`NOPASSWD`) on the control-plane node
- All k8s apt repos configured (v1.24–v1.36) at `/etc/apt/sources.list.d/kubernetes.list`
- NGX Storage backend reachable: supply `STORAGE_IPS`, `POOL_NAME` and `STORAGE_DATA_IP`
  for your own array; no lab endpoints are baked into these scripts.
- Runtime `STORAGE_API_KEY`; no key is stored in these scripts.
- `ALLOW_HOST_RESET=1` is required before the version script performs any
  host mutation. `DRIVER_MANIFEST_DIR` must identify the reviewed manifests.

## Scripts

- `run-version-test.sh <version>` — full, destructive test cycle for one k8s
  version. Scratch and logs use `$TMPDIR` or
  `$HOME/.cache/ngxstorage`.
- `capacity-check.sh <volumeHandle>` — queries the NGX Storage API for a
  share's used capacity (used after the `dd` write). Supply
  `STORAGE_API_KEY` in the environment; credentials are not stored in source.

## What each version tests

1. **Reset** — `kubeadm reset`, clear CNI/kubelet/etcd state.
2. **Install** — downgrade `kubeadm/kubectl/kubelet` to `<version>.*`.
3. **Init** — `kubeadm init --pod-network-cidr=10.244.0.0/16` (flannel default).
4. **CNI** — flannel via the latest release manifest.
5. **Driver** — apply the raw manifests from `deploy/manifest/kubernetes/`
   (driver image + latest upstream sidecars) + create the backend secret.
6. **PVC bind** — 1Gi RWO PVC must reach `Bound`.
7. **`dd` write** — busybox pod writes 100 MiB from `/dev/urandom`; verifies
   `100+0 records out` and the file size on the mounted volume.
8. **Capacity check** — reads the backend share's `capacity.used` via
   `GET /api/v2/share/{volumeHandle}`.

## Execution status

Do not run this from the CSI aggregate runner. It remains a manual,
environment-specific harness until the disposable-host target, manifest
provenance, teardown behavior, and package rollback are reviewed. Supply
`STORAGE_API_KEY` and `STORAGE_API_URL` through the runtime environment when
running the capacity helper; never put credentials in the script or command
history.

## Results (session 2026-08-12)

Driver image `quay.io/ngxstorage/nfs-csi-ngxstorage:2.0.1-1` + latest upstream
sidecars (provisioner v6.3.0, attacher v4.12.0, resizer v2.2.1, snapshotter
v8.6.0, registrar v2.17.0, livenessprobe v2.19.0).

Each version: PVC bind (1Gi RWO) → busybox `dd` 100 MiB → backend capacity
via storage API. `used=105046016B` confirms the 100 MiB write landed on the
NGX Storage backend (share baseline ~96 KiB).

| Version | Bind | dd write | Backend capacity |
|---------|:----:|:--------:|:----------------:|
| 1.24.17 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.25.16 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.26.15 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.27.16 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.28.13 | ✅ | ✅ 100 MiB @ 52.9 MB/s | 105046016 B |
| 1.29.15 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.30.14 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.31.14 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.32.13 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.33.13 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.34.10 | ✅ | ✅ 100 MiB | 105046016 B |
| 1.35.7  | ✅ | ✅ 100 MiB | 105046016 B |
| 1.36.3  | ✅ | ✅ 100 MiB | 105046016 B |

### Notes / gotchas discovered

- **Master taint key differs**: k8s ≤ 1.24 uses `node-role.kubernetes.io/master`;
  newer uses `node-role.kubernetes.io/control-plane`. The test script removes
  both so test pods can schedule on the single control-plane node.
- **Container runtime**: containerd v2.2.2 (CRI socket
  `/var/run/containerd/containerd.sock`) — no per-version change needed.
- **CNI**: flannel with `--pod-network-cidr=10.244.0.0/16` (flannel's default).
  Using another CIDR causes `subnet.env` failures.
- **Image pull**: the driver manifests must reference an image tag that exists
  on quay. A phantom tag (`2.0.2-1`) caused ImagePullBackOff; reverted to
  `2.0.1-1`.
- Rapid version churn can leave stale kubelet/containerd state; a clean
  `kubeadm reset` + `rm -rf /etc/cni /etc/kubernetes /var/lib/kubelet /var/lib/etcd`
  between runs is required.
