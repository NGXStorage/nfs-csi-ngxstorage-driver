# volume-access-readonly

Creates one RWX NFS PVC, writes a marker through a read-write pod, then mounts
the same PVC read-only in a second pod. The reader must read `writer.txt` and
reject a write attempt. Run in an isolated namespace and replace `default` in
the manifests before applying.
