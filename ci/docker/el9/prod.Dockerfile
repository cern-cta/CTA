# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Few notes on decisions that may seem strange at first sight:
# - We install and remove cta-release in the same layer to minimise image size. Putting it in the base image would increase the image size substantially
# - cta-release pulls in python, but microdnf does not autoremove it when uninstalling cta-release. Hence we remove python3* separately. Must happen in a separate remove as it complains otherwise (cta-release still needs it)
# - We don't care about systemd, so we remove it using rpm -e systemd-* --nodeps at the end. Ideally the base image doesn't contain systemd in the first place, but the layer in which we install the majority of the packages still pull in quite some systemd specific stuff
# - note on installing RPMs and multiple build contexts
# - no microdnf clean all because cache is mounted
# - note sharing=locked

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

# Core dependencies
RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    # Add some basic flags to all (micro)dnf commands to improve speed
    echo -e "[main]\ntsflags=nodocs\ninstall_weak_deps=False" > /etc/dnf/dnf.conf && \
    # Ensure consistent group and user for CTA services
    useradd -m -u 1000 -g tape cta && \
    # Ensure cta-versionlock can create the right versionlock file
    mkdir -p /etc/yum/pluginconf.d && \
    touch /etc/yum/pluginconf.d/versionlock.list && \
    # Ensure we can execute the script that installs packages
    chmod +x /usr/local/bin/build-service.sh && \
    # Create a .repo file pointing to the RPM repo we created in rep-builder
    echo -e "[cta]\nname=Repo containing CTA RPMS\nbaseurl=file:///mnt/rpms\ngpgcheck=0\nenabled=1\npriority=2" > /etc/yum.repos.d/cta.repo && \
    # Some basic utils (tar for kubectl cp and jq for many of the tests and convenience)
    microdnf install -y \
      tar \
      jq && \
    rm -rf /var/lib/dnf/history.*

###############################################
# SERVICE cta-taped
###############################################
FROM base AS cta-taped

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    /usr/local/bin/build-service.sh "cta-taped cta-tape-label cta-eosdf mt-st lsscsi sg3_utils"

# USER cta
CMD ["/usr/bin/cta-taped", "-c", "/etc/cta/cta-taped.conf", "--foreground", "--log-format=json", "--log-to-file=/var/log/cta/cta-taped.log"]

###############################################
# SERVICE cta-rmcd
###############################################
FROM base AS cta-rmcd

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    /usr/local/bin/build-service.sh "cta-rmcd cta-smc sg3_utils lsscsi mtx mt-st"

# USER cta
CMD ["/usr/bin/cta-rmcd", "-f", "/dev/smc"]

###############################################
# SERVICE cta-maintd
###############################################
FROM base AS cta-maintd

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    /usr/local/bin/build-service.sh "cta-maintd"

# USER cta
CMD ["/usr/bin/cta-maintd", "--foreground", "--log-to-file=/var/log/cta/cta-maintd.log", "--log-format", "json", "--config", "/etc/cta/cta-maintd.conf"]

###############################################
# SERVICE cta-frontend-grpc
###############################################
FROM base AS cta-frontend-grpc

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    /usr/local/bin/build-service.sh "cta-frontend-grpc krb5-workstation"

# USER cta
CMD ["/bin/bash", "-c", "/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend.log"]

###############################################
# SERVICE cta-frontend-xrd
###############################################
FROM base AS cta-frontend-xrd

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    /usr/local/bin/build-service.sh "cta-frontend krb5-workstation"

# USER cta
WORKDIR /home/cta
CMD ["xrootd", "-l", "/var/log/cta-frontend-xrootd.log", "-k", "fifo", "-n", "cta", "-c", "/etc/cta/cta-frontend-xrootd.conf", "-I", "v4"]

###############################################
# TOOLS cta-tools-grpc
###############################################
FROM base AS cta-tools-grpc

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    /usr/local/bin/build-service.sh "cta-admin-grpc cta-catalogue-utils cta-scheduler-utils krb5-workstation"

# USER cta
ENTRYPOINT ["/bin/bash"]

###############################################
# TOOLS cta-tools-xrd
###############################################
FROM base AS cta-tools-xrd

ARG USE_INTERNAL_REPOS
ARG USE_ORACLE_CATALOGUE

COPY etc/yum.repos.d-internal/ /tmp/internal-repos/

RUN --mount=type=bind,from=repo-builder,source=/rpms,target=/mnt/rpms \
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
    /usr/local/bin/build-service.sh "cta-cli cta-catalogue-utils cta-scheduler-utils krb5-workstation"

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
    --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    --mount=type=cache,target=/var/cache/yum,sharing=locked \
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

# FROM scratch AS build-all-stages

# COPY --from=cta-taped /etc/group .
# COPY --from=cta-maintd /etc/group .
# COPY --from=cta-rmcd /etc/group .
# COPY --from=cta-frontend-xrd /etc/group .
# COPY --from=cta-tools-xrd /etc/group .
