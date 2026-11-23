# Upstream Kubernetes deployment with Helm

This guide installs the NGX Storage NFS CSI driver on Kubernetes distributions without
OpenShift SCCs using the public
`ngxstorage/nfs-csi-ngxstorage-kubernetes` chart. Read
[REQUIREMENTS.md](../REQUIREMENTS.md) and
[COMPATIBILITY.md](../COMPATIBILITY.md) first. Prefer this method when you want
a packaged release with configurable values; use the
[raw manifests](../manifest/DEPLOYMENT.md) when you manage objects
declaratively. For OpenShift, use the
[OpenShift Helm guide](../../openshift/helm/DEPLOYMENT.md).

## 1. Prepare the node and image

Install the NFS client on every worker:

```bash
sudo apt-get install -y nfs-common       # Debian/Ubuntu
command -v mount.nfs
```

Use `nfs-utils` on RHEL-compatible systems. The chart defaults to
`quay.io/ngxstorage/nfs-csi-ngxstorage:2.0.1-1`; override `image.tag` only with
a reviewed immutable tag or digest, never `:latest`.

## 2. Create the backend Secret

The chart uses an existing Secret by default. Create it before installing:

```bash
kubectl -n nfs-csi-ngxstorage create secret generic nfs-csi-ngxstorage-config \
  --from-literal=storage-ips='192.0.2.10,192.0.2.11' \
  --from-literal=pool-name='pool-name' \
  --from-literal=api-key='<api-key>' \
  --from-literal=data-ip='198.51.100.10'
```

Prefer `--from-file` or an external secret manager when shell history is not
acceptable. Never commit credentials.

## 3. Render and install

Render and validate before installing:

```bash
helm repo add ngxstorage https://ngxstorage.github.io/helm
helm repo update
helm template ngx-nfs-csi ngxstorage/nfs-csi-ngxstorage-kubernetes \
  --set secret.name=nfs-csi-ngxstorage-config | \
  kubectl apply --dry-run=server -f -
```

Install it after creating the Secret:

```bash
helm upgrade --install ngx-nfs-csi ngxstorage/nfs-csi-ngxstorage-kubernetes \
  --namespace nfs-csi-ngxstorage --create-namespace \
  --set secret.name=nfs-csi-ngxstorage-config
```

To let Helm create the Secret, provide all four values. This stores the API
key in Helm release metadata, so use an external Secret for production:

```bash
helm upgrade --install ngx-nfs-csi ngxstorage/nfs-csi-ngxstorage-kubernetes \
  --namespace nfs-csi-ngxstorage --create-namespace \
  --set secret.create=true \
  --set secret.storageIPs='192.0.2.10,192.0.2.11' \
  --set secret.poolName='pool-name' \
  --set secret.apiKey='<api-key>' \
  --set secret.dataIP='198.51.100.10'
```

The chart also installs the driver StorageClass and snapshot class. The
`VolumeSnapshotClass` renders only when `snapshotClass.create` is true; set
`--set snapshotClass.create=false` when the cluster does not serve the
`snapshot.storage.k8s.io/v1` CRDs (see [COMPATIBILITY.md](../COMPATIBILITY.md)).

## 4. Validate

```bash
kubectl get csidriver nfs.csi.ngxstorage.com
kubectl -n nfs-csi-ngxstorage rollout status deployment/nfs-csi-ngxstorage-controller
kubectl -n nfs-csi-ngxstorage rollout status daemonset/nfs-csi-ngxstorage-node
kubectl get csinode
kubectl get storageclass nfs-csi-ngxstorage
```

Every node must have a ready node pod. Create a PVC and pod, wait for `Bound`
and `Running`, then write and read a file from the mount. Run the scenarios in
`docs/testing/scenario/GUIDE.md` one at a time.

## 5. Running two driver instances (1G + 40G backends)

A single cluster can host two NGX Storage CSI driver instances that serve different
backends. Each instance must advertise a **distinct CSI driver name** because
the `CSIDriver` object, kubelet plugin socket, StorageClass provisioner, and
VolumeSnapshotClass driver are all keyed by that name. Set `driverNamePrefix`
to give a second instance a distinct identity; the final driver name becomes
`<prefix>.nfs.csi.ngxstorage.com`. Leaving it empty preserves the canonical
`nfs.csi.ngxstorage.com`.

General-purpose 1G backend on the 1G NIC, reachable from every node:

```bash
helm upgrade --install ngx-nfs-csi-1g ngxstorage/nfs-csi-ngxstorage-kubernetes \
  --namespace nfs-csi-ngxstorage-1g --create-namespace \
  --set-string driverNamePrefix=201 \
  --set nodePlacement=all \
  --set secret.name=nfs-csi-ngxstorage-config \
  --set secret.create=true \
  --set secret.storageIPs='192.0.2.201,192.0.2.202' \
  --set secret.poolName='pool-1g' \
  --set secret.apiKey='<api-key>' \
  --set secret.dataIP='203.0.113.10'
```

High-speed 40G backend reachable only from worker-node 40G NICs; node pods are
restricted to workers with `nodePlacement=worker`:

```bash
helm upgrade --install ngx-nfs-csi-40g ngxstorage/nfs-csi-ngxstorage-kubernetes \
  --namespace nfs-csi-ngxstorage-40g --create-namespace \
  --set-string driverNamePrefix=205 \
  --set nodePlacement=worker \
  --set secret.name=nfs-csi-ngxstorage-config \
  --set secret.create=true \
  --set secret.storageIPs='198.51.100.205,198.51.100.206' \
  --set secret.poolName='pool-40g' \
  --set secret.apiKey='<api-key>' \
  --set secret.dataIP='203.0.113.20'
```

The chart renders StorageClass names and the node placement from
`driverNamePrefix` and `nodePlacement`; each instance must use a different
namespace. Verify both identities after install:

```bash
kubectl get csidriver
kubectl get storageclass
kubectl get pods -n nfs-csi-ngxstorage-1g
kubectl get pods -n nfs-csi-ngxstorage-40g
```

## 6. Troubleshooting and removal

- `timed out waiting for external-attacher` means the attach sidecar did not
  observe `VolumeAttachment.status.attached` before its deadline. Inspect
  `kubectl get volumeattachment` and controller sidecar logs; high parallel
  attach load can be transient.
- `FailedMount` is a node-side issue. Check the node pod, `mount.nfs`, and NFS
  data-IP reachability separately.
- Delete restore consumers before snapshot objects; NGX Storage rejects deletion of a
  snapshot that still has clone volumes.
- Set `logLevel=debug` during troubleshooting (`warn`/`error` reduce console
  output). Accepted values: `debug`, `info`, `warn`, `error`, `dpanic`,
  `panic`, `fatal`; the default is `info`.

Remove the release when no longer needed:

```bash
helm uninstall ngx-nfs-csi --namespace nfs-csi-ngxstorage
```

Do not remove snapshot CRDs or the snapshot controller if they are owned by
the Kubernetes platform.
