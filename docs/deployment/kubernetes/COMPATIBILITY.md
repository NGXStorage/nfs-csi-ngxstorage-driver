# Upstream Kubernetes compatibility

Upstream Kubernetes compatibility is distribution- and release-specific. The
Kubernetes manifest and Helm chart do not install OpenShift SCC resources.

## Supported contract

- Linux worker nodes with `mount.nfs` and privileged node mount access.
- Kubernetes 1.24 – 1.36 validated end-to-end (PVC bind, `dd` write, backend
  capacity) with kubeadm + containerd and the latest upstream CSI sidecars;
  see `tests/multi-version-k8s/`.
- CSI driver name: `nfs.csi.ngxstorage.com` (or `<prefix>.nfs.csi.ngxstorage.com`
  when `driverNamePrefix` is set).
- CSI v1.13 driver implementation with existing CSI v1 protobuf compatibility.
- Snapshot support is optional and requires the cluster's supported snapshot
  CRDs and snapshot controller. Set `snapshotClass.create=false` in the Helm
  chart when those APIs are unavailable.
- The backend Secret contains `storage-ips`, `pool-name`, `api-key`, and
  `data-ip`; controller and node consume the same Secret.

## Sidecars

The chart and manifests use the latest upstream CSI sidecars
(`registry.k8s.io/sig-storage/*`: csi-provisioner v6.3.0, csi-attacher v4.12.0,
csi-resizer v2.2.1, csi-snapshotter v8.6.0, csi-node-driver-registrar v2.17.0,
livenessprobe v2.19.0). These handle the driver's CSI 1.13 capability set and
do not require OCP payload credentials. Do not mix an OCP payload image with an
independently selected upstream sidecar without testing the complete set
together.

## Version review

Before deployment, record:

```bash
kubectl version
kubectl get crd volumesnapshotclasses.snapshot.storage.k8s.io \
  volumesnapshots.snapshot.storage.k8s.io \
  volumesnapshotcontents.snapshot.storage.k8s.io
```

Re-render the Helm chart and run the unit, sanity, and relevant live scenario
tests after changing the Kubernetes minor version or sidecar set.
