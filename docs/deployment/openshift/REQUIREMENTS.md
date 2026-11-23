# OpenShift requirements

- OpenShift 4.20.22, or another z-stream reviewed against
  `COMPATIBILITY.md`, on Linux amd64.
- RHCOS workers with `mount.nfs`; verify with `oc debug node/...`.
- Network access from every worker to the NGX Storage API and NFS data IPs. For a
  second backend (e.g. a 40G data path reachable only from worker NICs), the
  management API IPs must be routable from the control-plane nodes that run
  the controller. Management and data IPs can differ: the management API is
  used for provisioning, the data IP for NFS mounts.
- An NGX Storage pool with NFS enabled and a valid API key.
- A reviewed immutable driver image; do not deploy `:latest`.
- The platform CSI Snapshot Controller Operator and snapshot CRDs. This driver
  must not install or remove them.
- The node service account must receive the `privileged` SCC. The controller
  runs without host filesystem mounts and needs no additional SCC grant.
- The node profile keeps `hostNetwork`, `hostPID`, and `hostIPC` enabled for
  the validated NFSv3/`nolock` mount path. These are conservative compatibility
  settings aligned with the validated host integration and reference's NFS
  profile; the NGX Storage driver does not directly call PID/IPC namespace APIs. Do
  not remove them without revalidating NFSv3, static-PV, and stale-mount
  scenarios.
- A Secret named `nfs-csi-ngxstorage-config` containing `storage-ips`,
  `pool-name`, `api-key`, and `data-ip`, unless the Helm chart creates it.

Use raw manifests from `deploy/manifest/openshift/` or the public
`ngxstorage/nfs-csi-ngxstorage-openshift` chart. Multiple driver instances can
coexist on one cluster; each uses a distinct `driverNamePrefix` and
`namespace`. When a second instance runs in hostNetwork on the same workers,
give it distinct liveness ports (`nodeLivenessPort`, `controllerLivenessPort`).
See the Helm deployment guide.
