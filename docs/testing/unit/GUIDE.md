# Unit-test guide

Unit tests run in-process with fake backend and mount implementations. They do
not require a Kubernetes cluster, an NGX Storage appliance, or runtime credentials.

## Run all unit tests

```bash
GOCACHE="$HOME/.cache/ngxstorage/nfs-go-cache" go test ./...
```

Run a focused package or regression while developing:

```bash
GOCACHE="$HOME/.cache/ngxstorage/nfs-go-cache" go test ./pkg/driver -run TestNodePublishVolume -count=1
GOCACHE="$HOME/.cache/ngxstorage/nfs-go-cache" go test ./pkg/nfs -count=1
```

Before handing off a change, also run:

```bash
go vet ./...
git diff --check
```

## Scope

The tests cover request validation, CSI status mapping, capability advertising,
backend payloads, idempotency, NFS mount behavior, static PV preparation,
stale-mount recovery, and mutable parameter validation. Local csi-sanity is a
separate process-level suite; see `../sanity/GUIDE.md`.

Save run results outside the repository with the source/image identity, command, outcome and cleanup evidence.
