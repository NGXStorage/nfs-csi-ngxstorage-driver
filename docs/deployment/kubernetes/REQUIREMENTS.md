# Upstream Kubernetes requirements

- A supported Linux worker with `mount.nfs` (`nfs-common` on Debian/Ubuntu or
  `nfs-utils` on RHEL-compatible distributions).
- Kubernetes 1.24 through 1.36 is validated end-to-end (PVC bind, `dd` write,
  backend capacity) with kubeadm + containerd and the latest upstream CSI
  sidecars. See `tests/multi-version-k8s/`.
- Privileged node pods and host mount propagation allowed by cluster policy.
- The validated node profile also uses `hostNetwork`, `hostPID`, and `hostIPC`
  for the default NFSv3/`nolock` mount path. These are conservative
  compatibility settings rather than direct PID/IPC namespace API dependencies
  in the driver. Keep them unless an NFSv4-only profile is separately tested.
- Network access from every worker to the NGX Storage API and NFS data IPs. For a
  second backend (e.g. a 40G data path on a separate NIC), the management API
  IPs must be routable from the nodes that run the controller.
- An NGX Storage pool with NFS enabled and a valid API key.
- A reviewed immutable driver image; do not deploy `:latest`.
- Snapshot CRDs and a snapshot controller only when snapshot workflows are
  required. This driver does not install upstream snapshot infrastructure.
- A Secret named `nfs-csi-ngxstorage-config` containing `storage-ips`,
  `pool-name`, `api-key`, and `data-ip`, unless the Helm chart creates it.

Use raw manifests from `deploy/manifest/kubernetes/` or the public
`ngxstorage/nfs-csi-ngxstorage-kubernetes` chart. Multiple driver instances can
coexist on one cluster; each uses a distinct `driverNamePrefix` and `namespace`
(see the Helm deployment guide).
