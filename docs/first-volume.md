# Provision and mount a NFS volume

This example uses the StorageClass from the [manifest installation](deployment/README.md).
It provisions a 1 GiB share and mounts it in a temporary pod. Run the commands
in one Bash session and stop if a command fails. On OpenShift you can use `oc` in place of `kubectl`.

## Create the claim

Confirm the cluster context and StorageClass. The provisioner should be
`nfs.csi.ngxstorage.com`. The bundled class uses the `Delete` reclaim policy.

```bash
kubectl config current-context
kubectl get storageclass nfs-csi-ngxstorage
ns=$(kubectl create -f - -o jsonpath='{.metadata.name}' <<'YAML'
apiVersion: v1
kind: Namespace
metadata:
  generateName: nfs-volume-check-
YAML
)
```

```bash
kubectl -n "$ns" create -f - <<'YAML'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
spec:
  storageClassName: nfs-csi-ngxstorage
  accessModes: [ReadWriteMany]
  resources:
    requests:
      storage: 1Gi
YAML
```

## Mount the claim and check I/O

Use a BusyBox image available to your workers. If your cluster assigns a
UID or filesystem group, use the security context allowed for your workload.
OpenShift normally assigns these through its SCC. For Kubernetes namespaces
enforcing Restricted Pod Security, set `runAsNonRoot: true`, `runAsUser`
and `fsGroup` under `spec.securityContext` to permitted nonzero IDs. The
mounted directory must be writable by that user or group.

```bash
kubectl -n "$ns" create -f - <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: volume-check
spec:
  automountServiceAccountToken: false
  restartPolicy: Never
  securityContext:
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: client
      image: busybox:1.36.1
      command: ["sh", "-c", "sleep 3600"]
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: data
YAML
kubectl -n "$ns" wait --for=condition=Ready pod/volume-check --timeout=3m
kubectl -n "$ns" get pvc data -o wide
kubectl -n "$ns" exec volume-check -- sh -ec '
  printf "%s\n" volume-check > /data/marker
  sync
  test "$(cat /data/marker)" = volume-check
  echo "Readback OK"
'
```

If the claim stays `Pending`, inspect `kubectl -n "$ns" describe pvc data`
for provisioning events and review the StorageClass parameters for your array.
If the claim is `Bound` and the pod is waiting, inspect
`kubectl -n "$ns" describe pod volume-check`. Check the export permissions and the route from the scheduled worker to the NFS data IP. A working connection to the management API alone does not establish that the export can be mounted.

## Clean up

Capture the PV name before deleting the test claim. With the bundled class,
deleting the claim requests removal of its provisioned share. A class using
`Retain` leaves the PV and backing storage for the administrator.

```bash
pv=$(kubectl -n "$ns" get pvc data -o jsonpath='{.spec.volumeName}')
kubectl -n "$ns" delete pod volume-check --wait=true
kubectl -n "$ns" delete pvc data --wait=true
kubectl wait --for=delete "pv/$pv" --timeout=3m
kubectl delete namespace "$ns"
```

## Mount an existing NFS share

For an administrator-managed share, enable recursive export on the array.
Create a static PV with `persistentVolumeReclaimPolicy: Retain`. Set
`spec.csi.driver` to `nfs.csi.ngxstorage.com` and `spec.csi.volumeHandle` to
`nfs.csi.ngxstorage.com.<share-name>` using the array share name. The driver
mounts a child directory named after the PV. On an export that is already
read-only, that directory must exist before the pod mounts it.

Bind the PVC explicitly with `spec.volumeName` set to the PV name. Match
the storage class, capacity and access mode on both objects. For read-only
access, set `volumeAttributes.readonly: "true"` on the CSI source and
`readOnly: true` on the pod's volume mount. Verify by reading a known
file from the PV's directory. See the [static PV](../tests/scenario/volume-existing-static/volume-existing-static-pv.yaml)
and [bound PVC](../tests/scenario/volume-existing-static/volume-existing-static-pvc.yaml)
fixtures for the field layout.
