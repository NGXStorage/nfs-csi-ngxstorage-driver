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

## Kubernetes Compatibility

| Kubernetes Release | CSI Driver Version |
|--------------------|--------------------|
| 1.28+              | v2.0.0+            |

## Requirements

### Backend

- NGX Storage Manager with API v2 support
- A storage pool created and accessible
- NFS service enabled on the NGX appliance
- Network connectivity from Kubernetes worker nodes to the NGX NFS data IP

### Nodes

- Linux worker nodes (tested on Ubuntu, RHEL 9, Pardus)
- NFS client packages (`nfs-common` on Debian/Ubuntu, `nfs-utils` on RHEL)

### Permissions

- The driver runs with privileged security context on nodes (required for NFS mount operations via host chroot wrappers)
- HostPID, hostNetwork, and hostIPC are required for NFSv3/rpc-statd compatibility

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
  --set image.tag=2.0.0-2
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

| Parameter              | Default                                 | Description                              |
|------------------------|-----------------------------------------|------------------------------------------|
| `openshift.enabled`    | `false`                                 | Enable OpenShift SCC RBAC                |
| `image.repository`     | `quay.io/ngxstorage/nfs-csi-ngxstorage` | Driver image repository                  |
| `image.tag`            | `2.0.0-2`                               | Driver image tag                         |
| `image.pullPolicy`     | `IfNotPresent`                          | Image pull policy                        |
| `logLevel`             | `info`                                  | Driver log level (debug/info/warn/error) |
| `secret.create`        | `false`                                 | Create the backend secret via Helm       |
| `secret.name`          | `nfs-csi-ngxstorage-config`             | Secret name                              |
| `snapshotClass.create` | `true`                                  | Create VolumeSnapshotClass               |

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
