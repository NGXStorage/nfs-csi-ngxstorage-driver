# Local CSI sanity guide

`csi-sanity` checks the CSI gRPC contract against local controller and node
driver processes. It is not a Kubernetes-cluster test.

## Prerequisites

- A host that can reach the NGX Storage management and NFS data addresses.
- `STORAGE_API_KEY` supplied only through the environment.
- `nfs-common` (or the platform equivalent), mount privileges, and a writable
  `$HOME/.cache/ngxstorage` directory.

Build the local driver binary first. The csi-sanity binary is SHARED: the
runner builds it from the pinned fork commit (`tests/sanity/cloud/prebuilt.sh`
`csi_sanity_build_shared`, or a verified prebuilt when `CSI_PREBUILT_DIR` is
set) — it is no longer built from a driver-local `cmd/csi-sanity` copy.

```bash
TMPDIR="$HOME/.cache/ngxstorage" go build -o cmd/csi-driver/nfs-csi-ngxstorage-driver ./cmd/csi-driver
# Optional: build/inspect the shared csi-sanity binary manually:
TMPDIR="$HOME/.cache/ngxstorage" bash tests/sanity/build-shared-sanity.sh build/shared-sanity/csi-sanity
```

## Run one suite

Set the backend values in the environment, then run one suite at a time:

```bash
export STORAGE_API_KEY='<runtime secret>'
export STORAGE_IPS='<management IPs>'
export STORAGE_POOL_NAME='<pool>'
export STORAGE_DATA_IP='<NFS data IP>'
bash tests/run-all-tests.sh sanity local --allow-live
```

To investigate one spec without overlapping another suite:

```bash
FOCUS='ListVolumes.*absence of deleted' bash tests/run-all-tests.sh sanity local --allow-live
```

The harness starts exactly one local node process and one local controller
process, preserves their exit status, and removes only its own sockets and
mount directory. It writes logs under `tests/sanity/local/logs/`.

## Scope

The harness uses `github.com/kubernetes-csi/csi-test/v5/pkg/sanity` with mount
volumes. It exercises only the CSI capabilities advertised by this driver;
unsupported services such as GroupController are skipped by configuration.

Save run results outside the repository with the source/image identity, command, outcome and cleanup evidence.
