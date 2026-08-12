# NGX STORAGE NFS CSI DRIVER

A Container Storage Interface (CSI) Driver for NGX Storage NFS (Network File System). The CSI driver provisions NFS shares from an NGX Storage Manager backend and exposes them as PersistentVolumes in Kubernetes and OpenShift clusters.

## Features

| Feature                 | Status        | Notes                                |
|-------------------------|---------------|--------------------------------------|
| Dynamic provisioning    | Supported     | Creates NFS shares on NGX backend    |
| Volume expansion        | Supported     | Grow shares via PVC resize           |
| Snapshots & clones      | Supported     | Backend-native snapshot and clone    |
| Multi-node RWX          | Supported     | NFS shares mounted across nodes      |
| NFSv3, NFSv4.1, NFSv4.2 | Supported     | Configurable via `nfsvers` parameter |
| Volume attributes class | Supported     | Per-volume parameter overrides       |
| Thick/thin provisioning | Supported     | `thin_provision` parameter           |
| Read-only mounts        | Supported     | Via `PUBLISH_READONLY` capability    |
| Raw block volumes       | Not supported | NFS is a filesystem protocol         |

See [examples/](examples/) for end-to-end use cases.

### Volume Expansion

Update the storage request on an existing PVC:

```yaml
spec:
  resources:
    requests:
      storage: 200Gi  # increased from 100Gi
```

Important notes:

- Volumes can only be increased in size, not decreased.
- Expanding beyond the target capacity has no effect.
- Do not resize volumes directly on the NGX Storage Manager; use Kubernetes PVC resize.

### Snapshots

Create a VolumeSnapshot from a PVC and restore it to a new PVC:

```bash
kubectl apply -f examples/snapshot-restore/
```

See [examples/snapshot-restore/](examples/snapshot-restore/) for the complete workflow.

### Multi-Node ReadWriteMany (RWX)

NFS shares can be mounted by pods on different nodes simultaneously:

```yaml
spec:
  accessModes:
    - ReadWriteMany
```

See [examples/multi-pod-rwx/](examples/multi-pod-rwx/) for a working example.

### NFS Performance Tuning

For high-throughput workloads (10GbE+), the `nconnect` mount option
parallelizes NFS traffic across multiple TCP connections, delivering ~3× read
throughput on 40G links (≈4.7 GiB/s vs ≈1.5 GiB/s with a single connection).
See the NGX Storage knowledge base for detailed benchmarks and best
practices:

- [Improving NFS Read Performance: Overcoming Protocol Stack Challenges](https://www.ngxstorage.com/improving-nfs-read-performance-overcoming-protocol-stack-challenges/)
- [NFS Client Configuration Best Practices on Linux](https://kb.ngxstorage.com/knowledge-base/nfs-client-configuration-best-practices-on-linux/)

A pre-configured high-performance StorageClass (`nconnect=16`,
`rsize/wsize=1MiB`, `hard`) is at [examples/nfs-performance/](examples/nfs-performance/).

> `nconnect` requires Linux kernel 5.3+ on worker nodes. Start with
> `nconnect=8` on 10GbE; the kernel maximum is 16.

## Kubernetes Compatibility

Validated end-to-end (PVC bind → `dd` write → backend capacity) on every
patch release of Kubernetes 1.24 through 1.36 with kubeadm + containerd and
the latest upstream CSI sidecars.

| Kubernetes Release | CSI Driver Version | Validation |
|--------------------|--------------------|------------|
| 1.24 – 1.36        | v2.0.2+            | ✅ bind + dd + capacity |

OpenShift 4.20 validated on the same driver version.

## Requirements

### Backend

- NGX Storage Manager with API v2 support
- A storage pool created and accessible
- NFS service enabled on the NGX appliance
- Network connectivity from Kubernetes worker nodes to the NGX NFS data IP
- For a second backend (e.g. a 40G data path): its management API IPs must be
  routable from the cluster. Management and data IPs can differ; the
  management API is used for provisioning and the data IP for NFS mounts.

### Nodes

- Linux worker nodes (tested on Ubuntu 26.04, RHEL 9)
- NFS client packages (`nfs-common` on Debian/Ubuntu, `nfs-utils` on RHEL)
- `hostNetwork`/`hostPID`/`hostIPC` for the validated NFSv3/`nolock` path

### Permissions

- The driver runs with privileged security context on nodes (required for NFS mount operations via host chroot wrappers)
- On OpenShift, the node service account must use the `privileged` SCC (the
  chart and raw manifests grant this)

## Installing to Kubernetes

### 1. Clone the repository

```bash
git clone https://github.com/NGXStorage/nfs-csi-ngxstorage-driver
cd nfs-csi-ngxstorage-driver
```

### 2. Create the backend secret

Edit `deploy/kubernetes/nfs-csi-ngxstorage-secret.yaml` with your NGX Storage information:

```yaml
stringData:
  storage-ips: "192.168.1.201,192.168.1.202"
  pool-name: "POOLNAME"
  api-key: "your-api-key"
  data-ip: "10.10.10.10"
```

Apply the secret:

```bash
kubectl apply -f deploy/kubernetes/nfs-csi-ngxstorage-secret.yaml
```

### 3. Deploy the CSI driver

```bash
kubectl apply -f deploy/kubernetes/

# Expected output:
# namespace/nfs-csi-ngxstorage created
# serviceaccount/nfs-csi-ngxstorage-controller created
# serviceaccount/nfs-csi-ngxstorage-node created
# storageclass.storage.k8s.io/nfs-csi-ngxstorage created
# clusterrole.rbac.authorization.k8s.io/nfs-csi-ngxstorage-controller created
# clusterrolebinding.rbac.authorization.k8s.io/nfs-csi-ngxstorage-controller created
# deployment.apps/nfs-csi-ngxstorage-controller created
# daemonset.apps/nfs-csi-ngxstorage-node created
# csidriver.storage.k8s.io/nfs.csi.ngxstorage.com created
```

### 4. Verify the deployment

```bash
$ kubectl -n nfs-csi-ngxstorage get pods
NAME                                             READY   STATUS    RESTARTS   AGE
nfs-csi-ngxstorage-controller-xxxxxxxxxx-xxxxx   6/6     Running   0          30s
nfs-csi-ngxstorage-node-xxxxx                    3/3     Running   0          30s
```

### 5. Create a PVC and test

```bash
kubectl apply -f examples/single-pvc-single-pod/
```

Check that a PersistentVolume is provisioned:

```bash
$ kubectl get pv
NAME                                       CAPACITY   ACCESS MODES   STORAGECLASS         STATUS   AGE
pvc-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx   1Gi        RWX            nfs-csi-ngxstorage   Bound    10s
```

### 6. Write data to the volume

```bash
$ kubectl exec -ti my-csi-app -- sh
/ # touch /data/hello-world
/ # dd if=/dev/zero of=/data/test bs=1M count=100
/ # exit
$ kubectl exec -ti my-csi-app -- sh
/ # ls /data
hello-world  test
```

## Installing to OpenShift

### 1. Create the backend secret

```bash
oc create namespace nfs-csi-ngxstorage
oc -n nfs-csi-ngxstorage create secret generic nfs-csi-ngxstorage-config \
  --from-literal=storage-ips='192.168.1.201,192.168.1.202' \
  --from-literal=pool-name='POOLNAME' \
  --from-literal=api-key='your-api-key' \
  --from-literal=data-ip='10.10.10.10'
```

Optionally, if the cluster restricts the default SecurityContextConstraint, grant the node service account access to the privileged SCC:

```bash
oc adm policy add-scc-to-user privileged -z nfs-csi-ngxstorage-node -n nfs-csi-ngxstorage
```

### 2. Deploy the driver

```bash
oc apply -f deploy/openshift/
```

The OpenShift manifests include SCC RBAC bindings for the node DaemonSet.

### 3. Verify

```bash
oc -n nfs-csi-ngxstorage get pods
oc get storageclass nfs-csi-ngxstorage
```

## Helm Installation

A unified Helm chart is provided in `deploy/helm/chart/nfs-csi-ngxstorage/`. It supports both Kubernetes and OpenShift via an `openshift.enabled` toggle.

### From Helm repository (recommended)

```bash
helm repo add nfs-csi-ngxstorage https://ngxstorage.github.io/nfs-csi-ngxstorage-driver/
helm repo update
helm search repo nfs-csi-ngxstorage
```

**Kubernetes:**

```bash
helm install nfs-csi-ngxstorage nfs-csi-ngxstorage/nfs-csi-ngxstorage \
  --namespace nfs-csi-ngxstorage --create-namespace \
  --set secret.create=false \
  --set secret.name=nfs-csi-ngxstorage-config
```

**OpenShift:**

```bash
helm install nfs-csi-ngxstorage nfs-csi-ngxstorage/nfs-csi-ngxstorage \
  --namespace nfs-csi-ngxstorage --create-namespace \
  --set openshift.enabled=true \
  --set secret.create=false \
  --set secret.name=nfs-csi-ngxstorage-config
```

### From local chart directory

```bash
helm install nfs-csi-ngxstorage deploy/helm/chart/nfs-csi-ngxstorage/ \
  --namespace nfs-csi-ngxstorage --create-namespace \
  --set image.repository=quay.io/ngxstorage/nfs-csi-ngxstorage \
  --set image.tag=2.0.2-1
```

For OpenShift, add `--set openshift.enabled=true` to enable SCC RBAC bindings.

### Let Helm create the secret

```bash
helm install nfs-csi-ngxstorage deploy/helm/chart/nfs-csi-ngxstorage/ \
  --namespace nfs-csi-ngxstorage --create-namespace \
  --set secret.create=true \
  --set secret.storageIPs='192.168.1.201,192.168.1.202' \
  --set secret.poolName='POOLNAME' \
  --set secret.apiKey='your-api-key' \
  --set secret.dataIP='10.10.10.10'
```

### Helm values

| Parameter                 | Default                                 | Description                                        |
|---------------------------|-----------------------------------------|----------------------------------------------------|
| `openshift.enabled`       | `false`                                 | Enable OpenShift SCC RBAC                          |
| `image.repository`        | `quay.io/ngxstorage/nfs-csi-ngxstorage` | Driver image repository                            |
| `image.tag`               | `2.0.2-1`                               | Driver image tag                                   |
| `image.pullPolicy`        | `IfNotPresent`                          | Image pull policy                                  |
| `logLevel`                | `info`                                  | Driver log level (debug/info/warn/error)           |
| `driverNamePrefix`        | `""`                                    | Optional prefix → `<prefix>.nfs.csi.ngxstorage.com`|
| `namespace`               | `nfs-csi-ngxstorage`                    | Namespace for driver resources                     |
| `nodePlacement`           | `all`                                   | Node DaemonSet placement: `all`/`master`/`worker`  |
| `controllerPlacement`     | `master`                                | Controller placement: `master`/`worker`/`all`      |
| `controllerLivenessPort`  | `9808`                                  | Controller liveness probe port                     |
| `nodeLivenessPort`        | `9809`                                  | Node liveness probe port                           |
| `secret.create`           | `false`                                 | Create the backend secret via Helm                 |
| `secret.name`             | `nfs-csi-ngxstorage-config`             | Secret name                                        |
| `snapshotClass.create`    | `true`                                  | Create VolumeSnapshotClass                         |

### Running two driver instances (1G + 40G backends)

A single cluster can host two NGX CSI driver instances serving different
backends. Each must use a distinct `driverNamePrefix`, `namespace`, and set
`nodePlacement`/`controllerPlacement` as needed. The final driver name is
`<prefix>.nfs.csi.ngxstorage.com`; leave `driverNamePrefix` empty to keep the
canonical `nfs.csi.ngxstorage.com`.

1G backend (management `192.168.1.201/202`, pool `openstackSSD1`, data
`10.10.10.202`) on all nodes:

```bash
helm install nfs-csi-ngxstorage-1g deploy/helm/chart/nfs-csi-ngxstorage/ \
  --namespace nfs-csi-ngxstorage-1g --create-namespace \
  --set-string driverNamePrefix=201 \
  --set nodePlacement=all \
  --set secret.create=true \
  --set secret.storageIPs='192.168.1.201,192.168.1.202' \
  --set secret.poolName='openstackSSD1' \
  --set secret.dataIP='10.10.10.202' \
  --set secret.apiKey='<api-key>'
```

40G backend (management `192.168.1.205/206`, pool `TRNPOOL`, data
`10.10.20.206`) on worker nodes only. Liveness ports must differ so the two
hostNetwork node pods on the same worker do not collide:

```bash
helm install nfs-csi-ngxstorage-40g deploy/helm/chart/nfs-csi-ngxstorage/ \
  --namespace nfs-csi-ngxstorage-40g --create-namespace \
  --set-string driverNamePrefix=205 \
  --set nodePlacement=worker \
  --set controllerPlacement=master \
  --set nodeLivenessPort=9819 \
  --set controllerLivenessPort=9818 \
  --set secret.create=true \
  --set secret.storageIPs='192.168.1.205,192.168.1.206' \
  --set secret.poolName='TRNPOOL' \
  --set secret.dataIP='10.10.20.206' \
  --set secret.apiKey='<api-key>'
```

> Note: `--set-string` is required for numeric-looking prefixes (`201`, `205`);
> a plain `--set` coerces them to numbers.

## OpenShift Developer Catalog

To make the chart available in the OpenShift Developer Catalog (Topology → Add → Helm Chart):

### 1. Create a HelmChartRepository

```bash
# Cluster-wide (all projects):
oc apply -f deploy/helm/catalog/helm-chart-repository.yaml

# Per-project (single namespace):
oc apply -f deploy/helm/catalog/project-helm-chart-repository.yaml
```

Or create one manually pointing to the GitHub Pages URL:

```yaml
apiVersion: helm.openshift.io/v1beta1
kind: HelmChartRepository
metadata:
  name: nfs-csi-ngxstorage
spec:
  name: NGX Storage NFS CSI
  description: NGX Storage NFS CSI Driver Helm Charts
  disabled: false
  connectionConfig:
    url: https://ngxstorage.github.io/nfs-csi-ngxstorage-driver/
```

### 2. Install via Developer Catalog

In the OpenShift web console:

1. Navigate to **Developer** perspective → **+Add** → **Helm Chart**
2. Search for "NGX Storage"
3. Select **NGX Storage NFS CSI Driver**
4. Set `openshift.enabled=true` under YAML view
5. Configure the secret values and install

The chart will appear in the catalog once the `HelmChartRepository` is created and the index is reachable at the configured URL.

## StorageClass Parameters

| Parameter        | Default     | Description                        |
|------------------|-------------|------------------------------------|
| `blocksize`      | `128k`      | Block size for the share           |
| `thin_provision` | `on`        | Thin provisioning (on/off)         |
| `deduplication`  | `on`        | Data deduplication (on/off)        |
| `compression`    | `on`        | Data compression (on/off)          |
| `flash_cache`    | `off`       | Flash cache (on/off)               |
| `dram_cache`     | `off`       | DRAM cache (on/off)                |
| `permissions`    | `111111101` | NFS export permissions mask        |
| `nfsvers`        | (auto)      | NFS protocol version (3, 4.1, 4.2) |

## Uninstalling

```bash
# Delete all PVCs and PVs first
kubectl delete pvc --all -n <namespace>
# Then delete the driver
kubectl delete -f deploy/kubernetes/
```

For Helm:

```bash
helm uninstall nfs-csi-ngxstorage --namespace nfs-csi-ngxstorage
```

## Contributing

At NGX Storage we value our community! If you have issues or would like to contribute, open an issue or email [k8s@ngxstorage.com](mailto:k8s@ngxstorage.com).

## Licensing

Copyright 2023–2026 NGX Teknoloji A.Ş.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
