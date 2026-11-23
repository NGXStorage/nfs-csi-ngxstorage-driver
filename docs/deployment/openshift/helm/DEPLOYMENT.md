# OpenShift 4.20 deployment with Helm

This guide covers SCC admission, RHCOS host integration, and the platform-owned
snapshot controller when installing with the public
`ngxstorage/nfs-csi-ngxstorage-openshift` chart. Read
[REQUIREMENTS.md](../REQUIREMENTS.md) and `COMPATIBILITY.md` first. Use
the [raw manifests](../manifest/DEPLOYMENT.md) for declarative object
management. For upstream Kubernetes, use the
[Kubernetes Helm guide](../../kubernetes/helm/DEPLOYMENT.md).

The validated baseline is OpenShift 4.20.22 with Kubernetes payload 1.33.x,
three control-plane nodes, and two workers. Re-check the release image on any
other cluster:

```bash
release_image="$(oc get clusterversion version -o jsonpath='{.status.desired.image}')"
oc adm release info "$release_image"
```

## 1. Verify platform prerequisites

OpenShift owns the CSI Snapshot Controller Operator and snapshot CRDs. Verify
them without installing an upstream snapshot bundle:

```bash
oc api-resources --api-group=snapshot.storage.k8s.io
oc get clusteroperator storage
oc debug node/<worker> -- chroot /host command -v mount.nfs
```

Verify that every worker can reach the NGX Storage API and NFS data IPs.

## 2. Grant SCC access

The OpenShift chart includes the SCC-related RBAC that grants the node service
account use of the built-in privileged SCC. No manual `oc adm policy` command
is required; verify the RBAC after rendering:

```bash
helm repo add ngxstorage https://ngxstorage.github.io/helm
helm repo update
helm template ngx-nfs-csi ngxstorage/nfs-csi-ngxstorage-openshift | grep -A 12 'kind: ClusterRole'
```

The controller writes logs to its container stdout/stderr and does not mount
the host filesystem, so it needs no additional controller-specific SCC.
Do not grant `hostmount-anyuid` to the controller service account.

## 3. Create the backend Secret

```bash
oc -n nfs-csi-ngxstorage create secret generic nfs-csi-ngxstorage-config \
  --from-literal=storage-ips='192.0.2.10,192.0.2.11' \
  --from-literal=pool-name='pool-name' \
  --from-literal=api-key='<api-key>' \
  --from-literal=data-ip='198.51.100.10'
```

Keep the API key outside Git and prefer an external secret manager where
possible.

## 4. Render and install

The chart defaults to `quay.io/ngxstorage/nfs-csi-ngxstorage:2.0.1-1`; override
`image.tag` only with a reviewed immutable tag or digest. Render it before
installing and review the SCC/RBAC output:

```bash
helm template ngx-nfs-csi ngxstorage/nfs-csi-ngxstorage-openshift \
  --set secret.name=nfs-csi-ngxstorage-config | \
  oc apply --dry-run=server -f -
```

Install with an existing Secret:

```bash
helm upgrade --install ngx-nfs-csi ngxstorage/nfs-csi-ngxstorage-openshift \
  --namespace nfs-csi-ngxstorage --create-namespace \
  --set secret.name=nfs-csi-ngxstorage-config
```

Helm can create the Secret when explicitly requested:

```bash
helm upgrade --install ngx-nfs-csi ngxstorage/nfs-csi-ngxstorage-openshift \
  --namespace nfs-csi-ngxstorage --create-namespace \
  --set secret.create=true \
  --set secret.storageIPs='192.0.2.10,192.0.2.11' \
  --set secret.poolName='pool-name' \
  --set secret.apiKey='<api-key>' \
  --set secret.dataIP='198.51.100.10'
```

This places the API key in Helm release metadata; use an externally managed
Secret for production.

The driver installs only its external-snapshotter sidecar and
`VolumeSnapshotClass`; it does not install or remove the platform snapshot
CRDs, snapshot controller, or snapshot operator. The `VolumeSnapshotClass`
uses the `snapshot.storage.k8s.io/v1` API, where `driver` and `deletionPolicy`
are top-level fields, and renders only when `snapshotClass.create` is true.

## 5. Validate

```bash
oc get csidriver nfs.csi.ngxstorage.com
oc -n nfs-csi-ngxstorage rollout status deployment/nfs-csi-ngxstorage-controller
oc -n nfs-csi-ngxstorage rollout status daemonset/nfs-csi-ngxstorage-node
oc get csinode
oc get storageclass nfs-csi-ngxstorage
oc -n nfs-csi-ngxstorage get pods -o wide
```

Check SCC assignments:

```bash
oc -n nfs-csi-ngxstorage get pods \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.openshift\.io/scc}{"\n"}{end}'
```

Every node pod must be ready and the controller must have all six containers
ready. Then create a PVC and pod, verify data write/read, and run the scenarios
in `docs/testing/scenario/GUIDE.md` one at a time.

## 6. Troubleshooting

- `timed out waiting for external-attacher` means the attach sidecar did not
  observe `VolumeAttachment.status.attached` before its deadline. Inspect
  `oc get volumeattachment` and the external-attacher/driver logs. Large
  parallel attach campaigns can temporarily queue API updates.
- `FailedMount` is a node-side issue. Check the node pod, `mount.nfs`, the
  RHCOS host path, and NFS data-IP reachability.
- Node JSON logs are stored as `/var/log/ngxstorage/nfs/*.log` on the host;
  controller logs remain on stdout. Collect node files with:

  ```bash
  ./tests/tools/get-crash-logs.sh [destination-dir]
  ```

- Delete snapshot restore consumers before deleting snapshots. NGX Storage rejects a
  snapshot deletion while clone volumes exist; this is expected backend
  behavior.
- Set `logLevel=debug` during troubleshooting (`warn`/`error` reduce console
  output). Accepted values: `debug`, `info`, `warn`, `error`, `dpanic`,
  `panic`, `fatal`; the default is `info`.

## 7. Running two driver instances (1G + 40G backends)

A single cluster can host two NGX Storage CSI driver instances that serve different
backends. Each instance must advertise a **distinct CSI driver name** because
the `CSIDriver` object, kubelet plugin socket, StorageClass provisioner, and
VolumeSnapshotClass driver are all keyed by that name. Set `driverNamePrefix`
to give a second instance a distinct identity; the final driver name becomes
`<prefix>.nfs.csi.ngxstorage.com`. Leaving it empty preserves the canonical
`nfs.csi.ngxstorage.com`.

General-purpose 1G backend on the 1G NIC, reachable from every node:

```bash
helm upgrade --install ngx-nfs-csi-1g ngxstorage/nfs-csi-ngxstorage-openshift \
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
helm upgrade --install ngx-nfs-csi-40g ngxstorage/nfs-csi-ngxstorage-openshift \
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
oc get csidriver
oc get storageclass
oc get pods -n nfs-csi-ngxstorage-1g
oc get pods -n nfs-csi-ngxstorage-40g
```

## 8. Remove the driver

Delete workloads and PVCs first, and wait for VolumeAttachments to disappear:

```bash
helm uninstall ngx-nfs-csi --namespace nfs-csi-ngxstorage
oc get volumeattachment
```

Do not remove the OpenShift-owned snapshot CRDs, snapshot controller, or
`cluster-csi-snapshot-controller-operator`.
