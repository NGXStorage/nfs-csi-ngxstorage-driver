# Openshift manual deployment

Follow the maintained [manual deployment procedure](../../../../deploy/README.md)
from the NFS driver repository root; select `platform=openshift` and
`cli=oc`. The namespace is `ngxstorage-nfs`.
The procedure covers namespace/Secret creation, immutable image selection,
Kustomize render/server dry-run, apply, verification, rollback and uninstall.

Read [requirements](../REQUIREMENTS.md) and [compatibility](../COMPATIBILITY.md)
for platform prerequisites, and [image build/push](../../../../build/README.md)
for source commit and digest provenance. Platform snapshot components are
prerequisites and are not installed or removed by the driver bundle.
