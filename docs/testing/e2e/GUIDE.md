# Kubernetes E2E guide

This driver has two Kubernetes E2E suites. The external suite uses the
DriverDefinition in `tests/external-e2e/testdriver.yaml`; the custom suite is
the Go package under `tests/e2e/`. Both test a deployed driver against a real
Kubernetes cluster and are distinct from local csi-sanity.

## Prerequisites

- The driver, its StorageClass, and its VolumeSnapshotClass are deployed.
- `kubectl` can access the target cluster through `KUBECONFIG`.
- `E2E_TEST_BINARY` points to a Kubernetes `e2e.test` binary matching the
  target Kubernetes minor version.

## Run the external suite

```bash
export KUBECONFIG=/path/to/kubeconfig
export E2E_TEST_BINARY=/path/to/e2e.test
bash tests/external-e2e/run.sh
```

The runner verifies the deployed CSI driver and snapshot class, builds a
temporary DriverDefinition with an E2E-specific StorageClass, and runs only
`External.Storage` tests for `nfs.csi.ngxstorage.com`.

If a platform reports known non-ready control-plane conditions while worker
nodes are usable, set `E2E_ALLOWED_NOT_READY_NODES` to that explicit count.
For example, this OCP topology needs `3` for its tainted control-plane nodes.

The definition advertises only persistent mount-volume behavior, execution,
multi-pod RWX, snapshots, and controller expansion. Unsupported CSI features
are not advertised, so the upstream suite skips them.

## Run the custom suite

```bash
cd tests/e2e
CACHE_ROOT="$HOME/.cache/ngxstorage"
mkdir -p "$CACHE_ROOT"
E2E_RUN=1 KUBECONFIG=/path/to/kubeconfig \
  GOMODCACHE="$CACHE_ROOT/nfs-e2e-modcache" GOCACHE="$CACHE_ROOT/nfs-go-cache" \
  go test -v ./...
```

The custom suite covers dynamic provisioning and controller expansion. Set
`E2E_STATIC_VOLUME_ID`, `E2E_STATIC_SERVER`, and `E2E_STATIC_EXPORT` to enable
its static-PV case. `E2E_TEST_IMAGE` can select a cluster-available probe image.

Save run results outside the repository with the source/image identity, command, outcome and cleanup evidence.
