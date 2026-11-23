# NGX Storage NFS CSI driver versioning

This document covers the NFS CSI driver image. Quartet is a separate
product with its own version and release process.

Driver release tags follow the repository build convention:

```text
<VERSION>-<RELEASE>
```

`VERSION` identifies the driver semantic version; `RELEASE` increments when a
build changes without changing that semantic version. A published release tag
is immutable: do not overwrite it. Check `make docker-driver` / `make docker-push`, the Helm chart's
`appVersion`, and the exact image digest together before preparing a release;
this document intentionally does not assert which tag is currently deployed.
