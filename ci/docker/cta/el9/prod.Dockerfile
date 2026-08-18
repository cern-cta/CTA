# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Few notes on decisions that may seem strange at first sight:
# - Alma9-minimal vs alma. The minimal image is significantly smaller. The main difference is that it comes with microdnf instead of dnf
# - We install and remove cta-release in the same layer to minimise image size. Putting it in the base image would increase the image size substantially
# - note sharing=locked and id for concurrency (dnf caching is not thread safe)
# - AlmaLinux 9 and repository package versions intentionally float to receive upstream fixes on rebuild
# - dnf clean is unnecessary because the package caches are BuildKit cache mounts and are not committed to the image
# - rpm_context is an external BuildKit build context supplied by the build command
# - Containers log to stdout by default. The CI deployment overrides this command to exercise file logging and mirrors that file to stdout

# =========================================================================
# CERN CA CERTIFICATES
# =========================================================================
# hadolint ignore=DL3007
FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest AS cern-ca

# =========================================================================
# 1. REPO BUILDER
# Used to feed the RPMs to the other stages
# =========================================================================
# hadolint ignore=DL3007
FROM docker.io/almalinux/9-minimal:latest AS repo-builder

# hadolint ignore=DL3040,DL3041
RUN --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    # Add some basic flags to all (micro)dnf commands to improve speed
    printf '%s\n' '[main]' 'tsflags=nodocs' 'install_weak_deps=False' > /etc/dnf/dnf.conf && \
    microdnf install -y createrepo_c

# hadolint ignore=DL3022
COPY --from=rpm_context . /rpms

# Ensure this is recreated correctly.
RUN /bin/bash -o pipefail -c \
    'find /rpms -type f -name "*.rpm" -print0 | sort -z | xargs -0 sha256sum > /rpms/.rpm-hash && createrepo_c /rpms'

# =========================================================================
#  2. BASE IMAGE
# =========================================================================
# hadolint ignore=DL3007
FROM docker.io/almalinux/9-minimal:latest AS base

COPY build-service.sh /usr/local/bin/build-service.sh

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

# The upstream AlmaLinux image does not include the CERN root CAs so we copy them here explicitly
COPY --from=cern-ca /etc/ssl/certs/CERN-bundle.pem /etc/pki/ca-trust/source/anchors/CERN-bundle.pem

# Core dependencies
# hadolint ignore=DL3041
RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    # Ensure consistent user ID for CTA services
    # cta-common adds this user already, but it gives no guarantees on its ID, which we need to be stable for Kubernetes
    # Tape group already exists by default with gid 33
    useradd -m -u 1000 -g tape cta && \
    # Ensure cta-versionlock can update the versionlock file (file needs to exist)
    mkdir -p /etc/yum/pluginconf.d && \
    touch /etc/yum/pluginconf.d/versionlock.list && \
    # Ensure we can execute the script that installs packages
    chmod +x /usr/local/bin/build-service.sh && \
    # Create a .repo file pointing to the RPM repo we created in rep-builder
    printf '%s\n' '[cta]' 'name=Repo containing CTA RPMS' 'baseurl=file:///mnt/rpms' 'gpgcheck=0' 'enabled=1' 'priority=2' > /etc/yum.repos.d/cta.repo && \
    # Add some basic flags to all (micro)dnf commands to improve speed and reduce image size
    printf '%s\n' '[main]' 'tsflags=nodocs' 'install_weak_deps=False' > /etc/dnf/dnf.conf && \
    # Add the CERN root CAs to the system trust store before accessing package repositories
    update-ca-trust && \
    # Some basic utils (tar for kubectl cp, jq for many of the tests and convenience, procps-ng for some other utilities used in tests)
    # Requiring sudo is not ideal, but we need an update of the tests to be able to do without it.
    # Anyway, by setting allowPrivilegeEscalation: false in Kubernetes, sudo is unusable anyway
    microdnf install -y tar jq sudo procps-ng && \
    echo 'cta ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/cta && \
    chmod 0440 /etc/sudoers.d/cta && \
    # For runtime state configured with --runtime-dir
    install -d -o cta -g tape -m 0750 /run/cta && \
    # Cleanup
    rm -rf /var/lib/dnf/history.*

# =========================================================================
#  SERVICE cta-taped
# =========================================================================
FROM base AS cta-taped

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_SUPPORT

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-taped \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-taped \
    /usr/local/bin/build-service.sh "cta-taped cta-tape-label cta-external-tape-formats-test cta-eosdf mt-st lsscsi sg3_utils"

# See https://github.com/kubernetes/enhancements/blob/master/keps/sig-security/2763-ambient-capabilities/README.md
RUN setcap \
    cap_sys_rawio=+ep /usr/bin/cta-taped \
    cap_sys_rawio=+ep /usr/bin/cta-tape-label

USER cta
CMD ["/usr/bin/cta-taped", "-c", "/etc/cta/cta-taped.conf", "--foreground", "--log-format=json", "--stdout"]

# =========================================================================
#  SERVICE cta-rmcd
# =========================================================================
FROM base AS cta-rmcd

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_SUPPORT

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-rmcd \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-rmcd \
    /usr/local/bin/build-service.sh "cta-rmcd cta-smc sg3_utils lsscsi mtx mt-st"

USER cta
CMD ["/usr/bin/cta-rmcd", "-f", "/dev/smc"]

# =========================================================================
#  SERVICE cta-maintd
# =========================================================================
FROM base AS cta-maintd

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_SUPPORT

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-maintd \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-maintd \
    /usr/local/bin/build-service.sh "cta-maintd"

USER cta
CMD ["/usr/bin/cta-maintd", "--config-strict", "--config", "/etc/cta/cta-maintd.toml", "--runtime-dir", "/run/cta"]

# =========================================================================
#  SERVICE cta-frontend
# =========================================================================

# Once we split the RPMs, we should explicitly build the workflow-api and admin-api images here
FROM base AS cta-frontend

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_SUPPORT

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-frontend \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-frontend \
    # Remove the catalogue utils once the tests have been updated. For now, only the admin-api requires it
    /usr/local/bin/build-service.sh "cta-frontend-grpc cta-catalogue-utils krb5-workstation"

USER cta
CMD ["/usr/bin/cta-frontend-grpc"]

# =========================================================================
#  TOOLS cta-tools
# =========================================================================
FROM base AS cta-tools

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_SUPPORT
# CEPH is not required locally, so this allows us to disable it and reduce the image size for local dev workflows
ARG INSTALL_CEPH_COMMON=true

# There are two reasons why this image is huge:
# - eos-client: for now necessary as the system tests still assume the CTA and EOS rpms in one pod.
#   Once this assumption is removed from the system tests, we can migrate the client pod to use the
#   official EOS image and we don't need it here anymore
# - ceph-common: disabled for local development workflows, but required for the objectstore scheduler reset in CI
RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-tools \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-tools \
    packages="cta-admin-grpc cta-catalogue-utils cta-scheduler-utils \
      krb5-workstation cta-immutable-file-test eos-client xrootd-client \
      python3-xrootd bc" && \
    if [ "$INSTALL_CEPH_COMMON" = "true" ] || \
      [ "$INSTALL_CEPH_COMMON" = "1" ]; then \
      packages="$packages ceph-common"; \
    fi && \
    /usr/local/bin/build-service.sh "$packages" && \
    ln -sf /usr/bin/cta-admin-grpc /usr/bin/cta-admin

ENTRYPOINT ["/bin/bash"]

# =========================================================================
#  TOOLS cta-debug
# =========================================================================
FROM base AS cta-debug

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_SUPPORT

# This image is also gigantic, so we don't build it by default.
# In CI this is a manual job; for local development, use `cta-dev debug`
RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-debug \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-debug \
    /usr/local/bin/build-service.sh "cta-* cta-*-debuginfo* gdb"

ENTRYPOINT ["/bin/bash"]
