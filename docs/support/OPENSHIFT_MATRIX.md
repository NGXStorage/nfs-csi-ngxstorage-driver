# OpenShift support matrix

Only OpenShift versions with a complete Operator, CSI functional test, and
storage checkup result may be advertised as certified.

| OpenShift | Architecture | Status |
| --- | --- | --- |
| 4.20 | AMD64 / x86_64 | Certification target; validation pending |
| 4.21 | — | Not validated |
| 4.22 | — | Not validated |

The initial certified release supports `linux/amd64` only. Multi-arch
(`arm64`, `ppc64le`, `s390x`) is not advertised until the multi-arch driver
and Operator images, sidecar compatibility, and platform functional
certification exist.

When a minor release is added, run the Operator lifecycle, OpenShift CSI, and
KubeVirt storage checkup suites and update the CSV `com.redhat.openshift.versions`
annotation at the same time.
