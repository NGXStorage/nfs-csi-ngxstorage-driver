# Test NFS storage operations

For a first mount, use the [PVC and pod walkthrough](../../first-volume.md).
For repeatable storage tests, run the cases under `tests/scenario/` from the
repository root. List available cases without contacting a cluster:

```bash
bash tests/run-all-tests.sh list
```

## Run a case

Choose a test pool and supply the backend profile described in the
[test runner guide](../../../tests/README.md). Check the Kubernetes context,
then run one case at a time. For example, on an OpenShift test cluster:

```bash
oc config current-context
oc -n ngxstorage-nfs get deployment,daemonset
oc get csidriver nfs.csi.ngxstorage.com
BENCH=ocp2 bash tests/run-all-tests.sh scenario volume-lifecycle-rwo --allow-live
```

Use `BENCH=k8s` for the Kubernetes runner. Cases create real claims, shares or
snapshots. Use an isolated test pool. Worker reboot and stress runs require
their own runner options and a separate test window.

## Choose a case

| Operation | Case | What to check |
| --- | --- | --- |
| Provision and mount | `volume-lifecycle-rwo` | The claim binds and the pod can read its marker. |
| Share files across nodes | `volume-access-rwx-multinode` | Pods on different workers can read each other’s writes. |
| Enforce a single writer pod | `volume-access-rwop` | A second consumer cannot use the claim concurrently. |
| Mount using NFSv3 | `mount-nfs-v3` | Check mount options on the worker. |
| Grow a mounted filesystem | `volume-resize-online-filesystem` | Both claim capacity and the mounted filesystem grow. |
| Import a share | `volume-existing-static`, `volume-existing-readonly-static` | The writable mount accepts a marker. The read-only mount can read it and rejects a write. |
| Restore a snapshot | `volume-snapshot-restore` | The restored claim contains the data written before the snapshot. |

After each case, check that its pods, claims and attachments are gone. Verify
that the array removed the test-owned dynamic shares. The static cases
create their own fixture shares and remove them in their post-test step.
For snapshot restores, remove the restored volumes before deleting their
source snapshot.
