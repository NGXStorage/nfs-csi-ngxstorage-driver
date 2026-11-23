# Kubernetes deployment

Choose the maintained [manual manifest guide](manifest/DEPLOYMENT.md) or the
[Helm guide](helm/DEPLOYMENT.md). Read [requirements](REQUIREMENTS.md) and
[compatibility](COMPATIBILITY.md) first. The canonical manual commands live in
[deploy/README.md](../../../deploy/README.md); they use the current namespace,
Kustomize overlays and digest-pinned images.

For manual image build/push and source revision evidence, see
[build/README.md](../../../build/README.md). The platform snapshot controller
and CRDs are prerequisites; the driver does not install or remove them.
