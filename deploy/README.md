# NGX Storage NFS CSI manual deployment

Run these Bash commands from the driver repository root. The supported bundle
is `deploy/manifest/base` with Kubernetes and OpenShift overlays. For image
build/push and source provenance, see [build instructions](../build/README.md).
Helm installation is a separate workflow; do not install both over the same
CSI identity or let Helm adopt these objects implicitly.

## 1. Check prerequisites

Use kubectl for Kubernetes or oc for OpenShift, Python 3, and the existing
manifest validation dependencies. The cluster must provide the v1 snapshot
CRDs and snapshot controller before this complete bundle can be applied.
The bundle installs a snapshot sidecar/class, not the platform snapshot stack.
Check your context and cluster version before any mutation.

Workers need the NFS client (mount.nfs) and access to the export data address.
Verify NFS service/TCP reachability separately from the management HTTPS API.
NFS controller expansion and node capabilities must be read from this image;
do not infer them from another protocol.

Every node plugin needs the documented host root at /host, host device/sysfs
access and kubelet mount propagation. OpenShift installs the driver-specific
node SCC and grants use only to its node ServiceAccount. Do not grant the
controller the node SCC. OpenShift node placement is restricted to Linux workers;
check node labels before rollout. Upstream Kubernetes placement follows its
rendered overlay and must match the intended storage nodes.

All controller and node locations must reach both storage management HTTPS
addresses. Provide that reachability with your own network configuration
(routing, firewall and egress rules as needed). Supply your own management
addresses, storage pools and protocol settings through the documented runtime
configuration. Keep credentials in the configured Secret or local ignored
profile; do not add them to deployment manifests.

```bash
platform=openshift                   # or kubernetes
cli=oc                               # use kubectl with kubernetes
ns=ngxstorage-nfs
$cli config current-context
$cli version
$cli get nodes
$cli api-resources --api-group=snapshot.storage.k8s.io
export TMPDIR="$HOME/.cache/ngxstorage"
mkdir -p "$TMPDIR"
TMPDIR="$TMPDIR" bash deploy/test-manifests.sh --platform "$platform"
```

## 2. Create the namespace and storage Secret

For a new namespace:

```bash
$cli create namespace "$ns" --dry-run=client -o yaml | $cli apply -f -
secret_dir="$(mktemp -d "$TMPDIR/csi-secret.XXXXXX")"
chmod 700 "$secret_dir"
trap 'rm -rf "$secret_dir"' EXIT
read -r -s -p 'Storage API key: ' storage_api_key
printf '\n'
printf '%s' "$storage_api_key" > "$secret_dir/api-key"
unset storage_api_key
chmod 600 "$secret_dir/api-key"
$cli -n "$ns" create secret generic nfs-csi-ngxstorage-config \
  --from-file=api-key="$secret_dir/api-key" \
  --from-literal=storage-ips='192.0.2.10,192.0.2.11' \
  --from-literal=pool-name='pool-name' \
  --from-literal=data-ip='198.51.100.10'
rm -rf "$secret_dir"
trap - EXIT
```

Replace the non-secret example storage fields with your backend configuration.
The namespace and Secret name above match the checked-in overlay. Changing the
namespace also requires updating all namespaced references, including RBAC and
snapshot Secret references; changing only `ns` is insufficient. For an existing
Secret use your secret manager's update workflow and preserve its ownership;
do not dump it to a log or create a credential-bearing last-applied annotation.
The scripts do not automatically source a local .env file.

## 3. Select an image and render an immutable bundle

Use the registry digest verified by the image build/push workflow. Its OCI
revision must match the reviewed commit tag. `latest`, `dev` and branch tags
are not deployment inputs. Sidecars remain pinned to their reviewed digests.
The driver version and the image source commit are different identifiers.

```bash
DRIVER_IMAGE='quay.io/ngxstorage/nfs-csi-ngxstorage@sha256:REPLACE_WITH_VERIFIED_DIGEST'
image_repo="${DRIVER_IMAGE%@*}"
image_digest="${DRIVER_IMAGE##*@}"
[[ "$image_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo 'Verified digest required' >&2; exit 1; }
mkdir -p build/artifacts
bundle="$(mktemp -d "$PWD/build/artifacts/manual-$platform.XXXXXX")"
cp -R deploy/manifest "$bundle/manifest"
cat > "$bundle/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- manifest/$platform
images:
- name: quay.io/ngxstorage/nfs-csi-ngxstorage
  newName: $image_repo
  digest: $image_digest
EOF
$cli kustomize "$bundle" > "$bundle/rendered.yaml"
```

Inspect rendered namespaces, images, placement, SCC/RBAC, storage classes and
Lease configuration before apply. Save this ignored bundle as the deployment
and rollback input. It contains Secret references, not credentials.

On OpenShift, run the read-only ownership/SCC preflight for a first installation:

```bash
bash deploy/ocp-acceptance/ocp-acceptance-preflight.sh NFS --namespace "$ns"
```

This preflight checks the checked-in overlay; the following server dry-run
checks the selected image bundle. For an intentional existing installation,
review cluster-scoped resource ownership before explicitly using
`ALLOW_EXISTING_CLUSTER_OBJECTS=1`; do not use it to adopt another installation.

## 4. Dry-run, apply and verify

```bash
$cli apply --dry-run=server -f "$bundle/rendered.yaml"
$cli apply -f "$bundle/rendered.yaml"
$cli -n "$ns" rollout status deployment/nfs-csi-ngxstorage-controller --timeout=300s
$cli -n "$ns" rollout status daemonset/nfs-csi-ngxstorage-node --timeout=300s
$cli -n "$ns" get pods -o wide
$cli get csidriver nfs.csi.ngxstorage.com
$cli get csinode -o jsonpath='{range .items[*]}{.metadata.name}{": "}{.spec.drivers[*].name}{"\n"}{end}'
$cli get storageclass nfs-csi-ngxstorage
$cli get volumesnapshotclass nfs-csi-ngxstorage
```

For OpenShift, collect the stronger acceptance checks:

```bash
bash deploy/ocp-acceptance/ocp-acceptance-verify.sh NFS \
  --namespace "$ns" --expected-image "$DRIVER_IMAGE"
```

Confirm every intended worker registers the driver and all expected containers
stay ready without restarts. Check actual node SCC and sidecar leader Leases.
A runtime platform image digest can differ from an OCI index digest; resolve
that relationship before declaring a mismatch. A commit tag and a digest with
different source revisions are an error, even when both images exist.

Only after registration succeeds, run a protocol-specific PVC/pod scenario,
write/read and compare data, then verify normal unmount/delete cleanup. Pod
readiness alone is not successful provisioning, login, mount or data-path proof.

For a small PVC and pod you can use to verify the installation, follow
[Provision and mount a volume](../docs/first-volume.md).

## 5. Rollback and uninstall

Rollback uses the previous saved rendered bundle and matching backend Secret
configuration. Review state/coordination compatibility first, then apply that
bundle and repeat rollout, registration and workload checks. Never change the
Lease shard modulus during an ordinary rolling rollback or upgrade.

Before uninstall, retire consuming workloads and handle PVCs/snapshots according
to their retention policy. Wait for normal detach/unmount and VolumeAttachment
cleanup; do not force-delete PV/PVC finalizers to manufacture success.

```bash
$cli -n "$ns" scale deployment/nfs-csi-ngxstorage-controller --replicas=0
$cli -n "$ns" delete daemonset/nfs-csi-ngxstorage-node
$cli -n "$ns" wait --for=delete pod --all --timeout=300s
$cli delete -f "$bundle/rendered.yaml" --ignore-not-found
```

The bundle removes its SCC and cluster-scoped driver/class/RBAC objects; verify
no other installation shares them before deletion. Stop all driver pods before
removing coordination RBAC/Leases. Keep Secrets and namespaces until the desired
retention/cleanup is confirmed. Do not remove platform-owned snapshot CRDs,
snapshot controllers or operators. This procedure is not a storage data wipe.

## Distributed coordination

NFS controller/backend mutations are serialized across controller pods with a
fixed pool of pre-created `coordination.k8s.io/v1` Lease objects. The manual
bundle uses 128 reusable shards named `nfs-csi-ngxstorage-operation-lock-000`
through `127`. These are lock buckets, not consumable capacity: hash collisions
only serialize otherwise independent operations.

The controller operation-lock Role is restricted to the exact shard names and
only `get`, `update`, and `patch`. NFS node pods do not use distributed locks,
do not receive the coordination environment, and do not receive a projected
Kubernetes API credential. The controller ServiceAccount is not authorized to
use the node SCC.

Diagnose the pool with, for example,
`kubectl -n ngxstorage-nfs get lease nfs-csi-ngxstorage-operation-lock-000` (or
`oc` on OpenShift). Lease expiry is coordination, not backend fencing: shared
export/share state is re-read under the lock and uncertain state fails closed.

The coordination domain is guarded by the immutable
`nfs-csi-ngxstorage-coordination-config` ConfigMap. Do not change the shard
modulus during an ordinary rolling upgrade. A `128 -> 256` (or any other)
change requires a coordinated boundary: stop all NFS controller workloads,
verify no old controller pod remains, delete the immutable coordination
ConfigMap, render/apply the new complete Lease/RBAC/runtime configuration, then
start the new generation. Never run old and new shard counts concurrently.

For a coordination-safe manual uninstall, stop and wait for the controller/node
workloads first, then remove coordination RBAC, and remove the Lease
pool/coordination ConfigMap last. The Lease objects contain lock metadata only;
they do not contain backend share data or credentials.
