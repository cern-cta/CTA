# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Few notes on decisions that may seem strange at first sight:
# - Alma9-minimal vs alma. dnf vs microdnf
# - We install and remove cta-release in the same layer to minimise image size. Putting it in the base image would increase the image size substantially
# - cta-release pulls in python, but microdnf does not autoremove it when uninstalling cta-release. Hence we remove python3* separately. Must happen in a separate remove as it complains otherwise (cta-release still needs it)
# - We don't care about systemd, so we remove it using rpm -e systemd-* --nodeps at the end. Ideally the base image doesn't contain systemd in the first place, but the layer in which we install the majority of the packages still pull in quite some systemd specific stuff
# - note on installing RPMs and multiple build contexts
# - no microdnf clean all because cache is mounted
# - note sharing=locked and id for concurrency
# - At some point the USER should be uncommented. However, this can only happen after cta-taped and all tests have been updated to not rely on root-specific functionality.

###############################################
# 1. REPO BUILDER
# Used to feed the RPMs to the other stages
###############################################
FROM registry.cern.ch/docker.io/almalinux/9-minimal:latest AS repo-builder

RUN --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    # Add some basic flags to all (micro)dnf commands to improve speed
    echo -e "[main]\ntsflags=nodocs\ninstall_weak_deps=False" > /etc/dnf/dnf.conf && \
    microdnf install -y createrepo_c

COPY --from=rpm_context . /rpms

RUN createrepo_c /rpms

###############################################
# 2. BASE IMAGE
###############################################
FROM registry.cern.ch/docker.io/almalinux/9-minimal:latest AS base

COPY build-service.sh /usr/local/bin/build-service.sh

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

# Core dependencies
RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    # Ensure consistent group and user ID for CTA services
    # cta-common adds this user already, but it gives no guarantees on its ID, which we need to be stable for Kubernetes
    groupadd -g 2000 tape \
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
    # Some basic utils (tar for kubectl cp and jq for many of the tests and convenience)
    microdnf install -y \
      tar \
      jq && \
    # Cleanup
    rm -rf /var/lib/dnf/history.*

###############################################
# SERVICE cta-taped
###############################################
FROM base AS cta-taped

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-taped \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-taped \
    /usr/local/bin/build-service.sh "cta-taped cta-tape-label cta-eosdf mt-st lsscsi sg3_utils"

# USER cta
CMD ["/usr/bin/cta-taped", "-c", "/etc/cta/cta-taped.conf", "--foreground", "--log-format=json", "--log-to-file=/var/log/cta/cta-taped.log"]

###############################################
# SERVICE cta-rmcd
###############################################
FROM base AS cta-rmcd

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-rmcd \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-rmcd \
    /usr/local/bin/build-service.sh "cta-rmcd cta-smc sg3_utils lsscsi mtx mt-st"

# USER cta
CMD ["/usr/bin/cta-rmcd", "-f", "/dev/smc"]

###############################################
# SERVICE cta-maintd
###############################################
FROM base AS cta-maintd

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-maintd \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-maintd \
    /usr/local/bin/build-service.sh "cta-maintd"

USER cta
CMD ["/usr/bin/cta-maintd", "--log-file=/var/log/cta/cta-maintd.log", "--config-strict", "--config /etc/cta/cta-maintd.toml", "--runtime-dir /run/cta"]

###############################################
# SERVICE cta-frontend-grpc
###############################################
FROM base AS cta-frontend-grpc

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-frontend-grpc \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-frontend-grpc \
    /usr/local/bin/build-service.sh "cta-frontend-grpc cta-catalogue-utils krb5-workstation"
# TODO: remove the catalogue utils once the tests have been updated

USER cta
CMD ["/bin/bash", "-c", "/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend.log"]

###############################################
# SERVICE cta-frontend-xrd
###############################################
FROM base AS cta-frontend-xrd

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-frontend-xrd \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-frontend-xrd \
    /usr/local/bin/build-service.sh "cta-frontend cta-catalogue-utils krb5-workstation"
# TODO: remove the catalogue utils once the tests have been updated

USER cta
WORKDIR /home/cta
CMD ["xrootd", "-l", "/var/log/cta-frontend-xrootd.log", "-k", "fifo", "-n", "cta", "-c", "/etc/cta/cta-frontend-xrootd.conf", "-I", "v4"]

###############################################
# TOOLS cta-tools-grpc
###############################################
FROM base AS cta-tools-grpc

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-tools-grpc \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-tools-grpc \
    /usr/local/bin/build-service.sh "cta-admin-grpc cta-catalogue-utils cta-scheduler-utils krb5-workstation ceph-common"

# USER cta
ENTRYPOINT ["/bin/bash"]

###############################################
# TOOLS cta-tools-xrd
###############################################
FROM base AS cta-tools-xrd

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-tools-xrd \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-tools-xrd \
    /usr/local/bin/build-service.sh "cta-cli cta-catalogue-utils cta-scheduler-utils krb5-workstation ceph-common"

# USER cta
ENTRYPOINT ["/bin/bash"]

###############################################
# TOOLS cta-debug
###############################################
FROM base AS cta-debug

# The debug wildcard is a bit hacky. Should be replaced with something a bit more robust to ensure we only install actual CTA packages
# Also the repo to which this Dockerfile points in a local dev environment may not be accessible from the Kubernetes
# As such, there is not much point in keeping cta-release around (except wasting space), because downloads would fail anyway
RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,id=dnf-cta-debug \
    --mount=type=cache,target=/var/cache/yum,id=yum-cta-debug \
    microdnf install -y \
      cta-release && \
    cta-versionlock apply && \
    microdnf install -y --enablerepo crb \
      gdb \
      strace \
      valgrind \
      cta*debuginfo* \
    microdnf remove -y \
      cta-release && \
    rpm -e systemd-* --nodeps && \
    rm -rf /var/lib/dnf/history.*

ENTRYPOINT ["/bin/bash"]
