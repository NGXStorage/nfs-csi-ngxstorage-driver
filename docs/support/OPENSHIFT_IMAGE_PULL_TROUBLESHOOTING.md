# OpenShift image pull troubleshooting

Inspect the failing workload before changing registry or cluster settings.

```bash
oc -n <namespace> get pods
oc -n <namespace> describe pod <pod-name>
oc -n <namespace> get deployment <deployment-name> -o yaml
oc -n <namespace> get daemonset <daemonset-name> -o yaml
```

Check the image reference in the Pod spec and the reason reported by its
Events. A bare reference such as `driver:latest` does not identify your
intended registry. Use the complete repository reference; released manifests
should use the immutable image digest supplied with that release.

Verify that the referenced manifest exists in the selected registry and is
accessible from the nodes. For a private registry, verify the image pull
Secret reference and its namespace. Do not copy credential values into logs
or issue reports.

If the deployment relies on an OpenShift ImageStream, inspect its resolved
repository and tag:

```bash
oc -n <namespace> get imagestream <name> -o yaml
oc get configs.imageregistry.operator.openshift.io cluster -o yaml
```

An empty ImageStream repository or an unavailable registry needs to be
resolved with the cluster administrator. Reuse the approved registry channel
and then verify a new Pod reaches Ready with the expected image digest.
Changing cluster registry state is a separate administration operation.
