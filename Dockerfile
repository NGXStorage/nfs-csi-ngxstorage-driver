ARG UBI_IMAGE=registry.access.redhat.com/ubi9/ubi-minimal@sha256:34880b64c07f28f64d95737f82f891516de9a3b43583f39970f7bf8e4cfa48b7
FROM ${UBI_IMAGE}

ARG VERSION=dev
ARG RELEASE=dev
ARG VCS_REF=unknown

LABEL name="NGX Storage NFS CSI Driver" \
      maintainer="NGX Storage" \
      vendor="NGX Storage" \
      version="${VERSION}" \
      release="${RELEASE}" \
      summary="NGX Storage NFS CSI Driver" \
      description="Container Storage Interface driver for NGX Storage NFS volumes" \
      org.opencontainers.image.title="NGX Storage NFS CSI Driver" \
      org.opencontainers.image.vendor="NGX Storage" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="https://github.com/NGXStorage/nfs-csi-ngxstorage-driver" \
      vcs-ref="${VCS_REF}" \
      vcs-type="git"

# Keep the runtime image minimal. Host mount commands are executed through the
# /host chroot wrapper; util-linux is still required for chroot and findmnt.
RUN microdnf install -y \
      bash \
      util-linux \
      ca-certificates \
    && microdnf clean all \
    && mkdir -p /chroot /licenses

COPY [ "cmd/chroot-host-wrapper.sh", "/chroot/chroot-host-wrapper.sh" ]
COPY [ "cmd/entrypoint.sh", "/entrypoint.sh" ]
COPY [ "cmd/csi-driver/nfs-csi-ngxstorage-driver", "/usr/local/bin/nfs-csi-ngxstorage-driver" ]
COPY LICENSE /licenses/LICENSE

RUN chmod +x /usr/local/bin/nfs-csi-ngxstorage-driver \
    && chmod +x /entrypoint.sh \
    && chmod +x /chroot/chroot-host-wrapper.sh \
    && ln -s /chroot/chroot-host-wrapper.sh /chroot/free \
    && ln -s /chroot/chroot-host-wrapper.sh /chroot/stat \
    && ln -s /chroot/chroot-host-wrapper.sh /chroot/findmnt \
    && ln -s /chroot/chroot-host-wrapper.sh /chroot/mount \
    && ln -s /chroot/chroot-host-wrapper.sh /chroot/umount

ENV PATH="/chroot:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# The node operand genuinely requires root/privileged host integration for
# mount propagation, SYS_ADMIN/SYS_CHROOT, /host, and kubelet access.
ENTRYPOINT [ "/entrypoint.sh" ]
