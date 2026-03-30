# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

###############################################
# BASE IMAGE
###############################################
FROM docker.io/almalinux/9-minimal:latest AS base

ARG CTA_REPO_URL
ENV CTA_REPO_URL=${CTA_REPO_URL}

# Internal repos
COPY ci/docker/el9/etc/yum.repos.d-internal/* /etc/yum.repos.d/

# Core dependencies
RUN microdnf install -y --nodocs --setopt='install_weak_deps=0' \
      tar \
      jq && \
    useradd -m -u 1000 -g tape cta && \
    echo -e "[cta]\n\
name=Repo containing CTA RPMS\n\
baseurl=${CTA_REPO_URL}\n\
gpgcheck=0\n\
enabled=1\n\
priority=2" > /etc/yum.repos.d/cta.repo && \
    mkdir -p /etc/yum/pluginconf.d && touch /etc/yum/pluginconf.d/versionlock.list

###############################################
# SERVICE cta-taped
###############################################
FROM base AS cta-taped

RUN microdnf install -y --nodocs --setopt='install_weak_deps=0' \
      epel-release \
      cta-release && \
    cta-versionlock apply && \
    microdnf install -y --nodocs --setopt='install_weak_deps=0' --enablerepo crb \
      mt-st \
      lsscsi \
      sg3_utils \
      cta-taped \
      cta-tape-label \
      cta-eosdf && \
    microdnf remove -y \
      cta-release \
      epel-release && \
    microdnf clean all

CMD ["/usr/bin/cta-taped", "-c", "/etc/cta/cta-taped.conf", "--foreground", "--log-format=json", "--log-to-file=/var/log/cta/cta-taped.log"]

###############################################
# SERVICE cta-rmcd
###############################################
FROM base AS cta-rmcd

RUN microdnf install -y --nodocs --setopt='install_weak_deps=0' \
      epel-release \
      cta-release && \
    cta-versionlock apply && \
    microdnf install -y --nodocs --setopt='install_weak_deps=0' --enablerepo crb \
      mt-st \
      mtx \
      lsscsi \
      sg3_utils \
      cta-rmcd \
      cta-smc && \
    microdnf remove -y \
      cta-release \
      epel-release && \
    microdnf clean all

CMD ["/usr/bin/cta-rmcd", "-f", "/dev/smc"]

###############################################
# SERVICE cta-maintd
###############################################
FROM base AS cta-maintd

RUN microdnf install -y --nodocs --setopt='install_weak_deps=0' \
      epel-release \
      cta-release && \
    cta-versionlock apply && \
    microdnf install -y --nodocs --setopt='install_weak_deps=0' --enablerepo crb \
      cta-maintd && \
    microdnf remove -y \
      cta-release \
      epel-release && \
    microdnf clean all

# USER cta
CMD ["/usr/bin/cta-maintd", "--foreground", "--log-to-file=/var/log/cta/cta-maintd.log", "--log-format", "json", "--config", "/etc/cta/cta-maintd.conf"]

###############################################
# SERVICE cta-frontend-grpc
###############################################
FROM base AS cta-frontend-grpc

RUN microdnf install -y --nodocs --setopt='install_weak_deps=0' \
      epel-release \
      cta-release && \
    cta-versionlock apply && \
    microdnf install -y --nodocs --setopt='install_weak_deps=0' --enablerepo crb \
      cta-frontend-grpc \
      krb5-workstation && \
    microdnf remove -y \
      cta-release \
      epel-release && \
    microdnf clean all

# USER cta
CMD ["/bin/bash", "-c", "/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend.log"]

###############################################
# SERVICE cta-frontend-xrd
###############################################
FROM base AS cta-frontend-xrd

RUN microdnf install -y --nodocs --setopt='install_weak_deps=0' \
      epel-release \
      cta-release && \
    cta-versionlock apply && \
    microdnf install -y --nodocs --setopt='install_weak_deps=0' --enablerepo crb \
      cta-frontend \
      krb5-workstation && \
    microdnf remove -y \
      cta-release \
      epel-release && \
    microdnf clean all

# USER cta
WORKDIR /home/cta
CMD ["xrootd", "-l", "/var/log/cta-frontend-xrootd.log", "-k", "fifo", "-n", "cta", "-c", "/etc/cta/cta-frontend-xrootd.conf", "-I", "v4"]

###############################################
# TOOLS cta-tools-grpc
###############################################
FROM base AS cta-tools-grpc

RUN microdnf install -y --nodocs --setopt='install_weak_deps=0' \
      epel-release \
      cta-release && \
    cta-versionlock apply && \
    microdnf install -y --nodocs --setopt='install_weak_deps=0' --enablerepo crb \
      cta-admin-grpc \
      cta-catalogueutils \
      cta-objectstore-tools \
      krb5-workstation && \
    microdnf remove -y \
      cta-release \
      epel-release && \
    microdnf clean all

# USER cta
ENTRYPOINT ["/bin/bash"]

###############################################
# TOOLS cta-tools-xrd
###############################################
FROM base AS cta-tools-xrd

RUN microdnf install -y --nodocs --setopt='install_weak_deps=0' \
      epel-release \
      cta-release && \
    cta-versionlock apply && \
    microdnf install -y --nodocs --setopt='install_weak_deps=0' --enablerepo crb \
      cta-cli \
      cta-catalogueutils \
      cta-objectstore-tools \
      krb5-workstation && \
    microdnf remove -y \
      cta-release \
      epel-release && \
    microdnf clean all

# USER cta
ENTRYPOINT ["/bin/bash"]
