# Deployment guides

These guides separate the NGX Storage NFS CSI deployment by platform and by install
method. Start with the platform requirements, then choose the method that fits
your environment. Each platform directory contains its own compatibility
contract.

## Platform and method matrix

| Platform | Method | Guide |
| --- | --- | --- |
| Upstream Kubernetes | Raw manifests | [kubernetes/manifest/DEPLOYMENT.md](kubernetes/manifest/DEPLOYMENT.md) |
| Upstream Kubernetes | Helm | [kubernetes/helm/DEPLOYMENT.md](kubernetes/helm/DEPLOYMENT.md) |
| Red Hat OpenShift 4.20 | Raw manifests | [openshift/manifest/DEPLOYMENT.md](openshift/manifest/DEPLOYMENT.md) |
| Red Hat OpenShift 4.20 | Helm | [openshift/helm/DEPLOYMENT.md](openshift/helm/DEPLOYMENT.md) |

Prerequisites and support boundaries live in each platform directory:

| Platform | Requirements | Compatibility |
| --- | --- | --- |
| Upstream Kubernetes | [kubernetes/REQUIREMENTS.md](kubernetes/REQUIREMENTS.md) | [kubernetes/COMPATIBILITY.md](kubernetes/COMPATIBILITY.md) |
| Red Hat OpenShift 4.20 | [openshift/REQUIREMENTS.md](openshift/REQUIREMENTS.md) | [openshift/COMPATIBILITY.md](openshift/COMPATIBILITY.md) |

## Choosing a method

- **Helm** packages the driver with configurable values (`image.tag`,
  `secret.name`, `logLevel`, `snapshotClass.create`).
- **Raw manifests** are for declarative object management and environments
  without Helm.

## Common rules

- Raw manifests are under `deploy/manifest/{kubernetes,openshift}`.
- Helm charts are maintained in the public [NGX Storage Helm repository](https://github.com/NGXStorage/helm), separately from this driver repository. Add it with `helm repo add ngxstorage https://ngxstorage.github.io/helm`, then install `ngxstorage/nfs-csi-ngxstorage-kubernetes` or `ngxstorage/nfs-csi-ngxstorage-openshift` as appropriate.
- Driver deployment images are pinned to a verified digest. The matching
  commit tag identifies source; latest/dev and branch tags are not deployment
  inputs. See [manual installation](../../deploy/README.md) and
  [image build/push](../../build/README.md).
- Keep `storage-ips`, `pool-name`, `api-key`, and `data-ip` in a Secret. Helm
  can reference an existing Secret or create one when explicitly requested.
- The `VolumeSnapshotClass` uses the `snapshot.storage.k8s.io/v1` API, where
  `driver` and `deletionPolicy` are top-level fields. Apply it only when the
  cluster provides the snapshot CRDs and controller.
- Verify the controller and node rollout before creating a PVC.
- NGX Storage does not delete a snapshot while clone volumes exist. Delete restore
  consumers before deleting the snapshot object; classify this backend response
  as expected behavior.

## Directory layout

```text
docs/deployment/
├── README.md
├── kubernetes/
│   ├── REQUIREMENTS.md
│   ├── COMPATIBILITY.md
│   ├── manifest/
│   │   └── DEPLOYMENT.md
│   └── helm/
│       └── DEPLOYMENT.md
└── openshift/
    ├── REQUIREMENTS.md
    ├── COMPATIBILITY.md
    ├── manifest/
    │   └── DEPLOYMENT.md
    └── helm/
        └── DEPLOYMENT.md

deploy/
└── manifest/
    ├── kubernetes/
    └── openshift/
```
