# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Few notes on decisions that may seem strange at first sight:
# - Alma9-minimal vs alma. The minimal image is significantly smaller. The main difference is that it comes with microdnf instead of dnf
# - We install and remove cta-release in the same layer to minimise image size. Putting it in the base image would increase the image size substantially
# - note sharing=locked and id for concurrency (dnf caching is not thread safe)

# =========================================================================
# 1. REPO BUILDER
# Used to feed the RPMs to the other stages
# =========================================================================
FROM registry.cern.ch/docker.io/almalinux/9-minimal:latest AS repo-builder

RUN --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    # Add some basic flags to all (micro)dnf commands to improve speed
    echo -e "[main]\ntsflags=nodocs\ninstall_weak_deps=False" > /etc/dnf/dnf.conf && \
    microdnf install -y createrepo_c

COPY --from=rpm_context . /rpms

RUN createrepo_c /rpms

# =========================================================================
# 2. BASE IMAGE
# =========================================================================
FROM registry.cern.ch/docker.io/almalinux/9-minimal:latest AS base

COPY build-service.sh /usr/local/bin/build-service.sh

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

# Core dependencies
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
    echo -e "[cta]\nname=Repo containing CTA RPMS\nbaseurl=file:///mnt/rpms\ngpgcheck=0\nenabled=1\npriority=2" > /etc/yum.repos.d/cta.repo && \
    # Add some basic flags to all (micro)dnf commands to improve speed and reduce image size
    echo -e "[main]\ntsflags=nodocs\ninstall_weak_deps=False" > /etc/dnf/dnf.conf && \
    # Some basic utils (tar for kubectl cp, jq for many of the tests and convenience)
    # Requiring sudo is not ideal, but we need an update of the tests to be able to do without it.
    # Anyway, by setting allowPrivilegeEscalation: false in Kubernetes, sudo is unusable anyway
    microdnf install -y tar jq sudo && \
    echo 'cta ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/cta && \
    chmod 0440 /etc/sudoers.d/cta && \
    # Cleanup
    rm -rf /var/lib/dnf/history.*

# =========================================================================
# SERVICE cta-taped
# =========================================================================
FROM base AS cta-taped

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-taped \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-taped \
    /usr/local/bin/build-service.sh "cta-taped cta-tape-label cta-external-tape-formats-test cta-eosdf mt-st lsscsi sg3_utils"

# Can be uncommented once we remove cta-taped setting its own process capabilities
# USER cta
CMD ["/usr/bin/cta-taped", "-c", "/etc/cta/cta-taped.conf", "--foreground", "--log-format=json", "--log-to-file=/var/log/cta/cta-taped.log"]

# =========================================================================
# SERVICE cta-rmcd
# =========================================================================
FROM base AS cta-rmcd

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-rmcd \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-rmcd \
    /usr/local/bin/build-service.sh "cta-rmcd cta-smc sg3_utils lsscsi mtx mt-st"

USER cta
CMD ["/usr/bin/cta-rmcd", "-f", "/dev/smc"]

# =========================================================================
# SERVICE cta-maintd
# =========================================================================
FROM base AS cta-maintd

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-maintd \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-maintd \
    /usr/local/bin/build-service.sh "cta-maintd"

USER cta
CMD ["/usr/bin/cta-maintd", "--log-file=/var/log/cta/cta-maintd.log", "--config-strict", "--config /etc/cta/cta-maintd.toml", "--runtime-dir /run/cta"]

# =========================================================================
# SERVICE cta-frontend
# =========================================================================
# TODO: once we split the RPMs, we should explicitly build the workflow-api and admin-api images here
FROM base AS cta-frontend

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-frontend \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-frontend \
    # Remove the catalogue utils once the tests have been updated. For now, only the admin-api requires it
    /usr/local/bin/build-service.sh "cta-frontend-grpc cta-catalogue-utils krb5-workstation"

USER cta
CMD ["/bin/bash", "-c", "/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend.log"]

# =========================================================================
# TOOLS cta-tools
# =========================================================================
FROM base AS cta-tools

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_CATALOGUE

# This image is gigantic... Find a way to reduce it..
# Sadly we need eos-client here for the CI, which bloat the image by quite a bit.
# Ideally the client chart uses the EOS image so that we don't need eos-client here. However, that requires
# the completion of the system test migration to ensure we remove the assumption that the eos-client and cta-admin RPMs exist in the same container.
RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-tools \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-tools \
    /usr/local/bin/build-service.sh "cta-admin-grpc cta-catalogue-utils cta-scheduler-utils krb5-workstation ceph-common cta-immutable-file-test eos-client xrootd-client bc" && \
    ln -sf /usr/bin/cta-admin-grpc /usr/bin/cta-admin

ENTRYPOINT ["/bin/bash"]

# =========================================================================
# TOOLS cta-debug
# =========================================================================
FROM base AS cta-debug

ARG ENABLE_INTERNAL_REPOS
ARG ENABLE_ORACLE_CATALOGUE

# This image is also gigantic, so we don't build it by default. Build this using the --enable-debug-image in build_deploy.sh
RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-tools \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-tools \
    /usr/local/bin/build-service.sh "cta-* cta-debuginfo-* gdb"

ENTRYPOINT ["/bin/bash"]
