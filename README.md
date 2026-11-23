# NGX Storage NFS CSI Driver

The NGX Storage NFS CSI driver provisions NFS shares on the array and mounts them on Linux workers. Use a ReadWriteMany claim when pods on several nodes need access to the same files.

## Features

- Dynamic provisioning of NFS shares from a selected pool.
- ReadWriteMany volumes shared across nodes.
- Volume expansion and snapshots through the CSI API.
- Mounting an existing export without provisioning a new share.
- Kubernetes and OpenShift deployments with manifests or Helm charts.
- Standalone build from the committed Go dependencies.

## Prerequisites

Enable NFS on the array and select a share pool. Workers need an NFS mount helper and a route to the NFS data IP. Both the controller and node plugins also need access to the array management API.

| Platform | Requirements |
| --- | --- |
| Kubernetes | [Worker and storage requirements](docs/deployment/kubernetes/REQUIREMENTS.md) |
| OpenShift | [Worker, storage and SCC requirements](docs/deployment/openshift/REQUIREMENTS.md) |

## Install

| Platform | Installation |
| --- | --- |
| Kubernetes | [Manifests](docs/deployment/kubernetes/manifest/DEPLOYMENT.md) · [Helm](docs/deployment/kubernetes/helm/DEPLOYMENT.md) |
| OpenShift | [Manifests](docs/deployment/openshift/manifest/DEPLOYMENT.md) · [Helm](docs/deployment/openshift/helm/DEPLOYMENT.md) |

The manifest bundle registers `nfs.csi.ngxstorage.com` and creates the StorageClass `nfs-csi-ngxstorage`.

## First volume

Once the controller and node pods are ready, [provision a volume and verify that a pod can write to and read from it](docs/first-volume.md). For an export that already exists, follow [Mount an existing NFS share](docs/first-volume.md#mount-an-existing-nfs-share).

## Build and test

Run `make build` to compile the driver from the committed Go dependencies. For container images, test commands and configuration, see the [build guide](build/README.md), the [test guide](tests/README.md) and the [documentation index](docs/README.md).

## License

Licensed under [Apache 2.0](LICENSE).
