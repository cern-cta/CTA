# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest AS base

ENV CTA_REPO_DIR="/opt/repo"
ENV LOCAL_RPM_DIR="image_rpms"

# Common repo configuration
COPY continuousintegration/docker/el9/etc/yum.repos.d-internal/* /etc/yum.repos.d/

# Core dependencies
RUN dnf install -y \
      python3-dnf-plugin-versionlock \
      dnf-utils \
      createrepo \
      epel-release \
      gdb \
      jq \
      krb5-workstation && \
    chmod 0644 /etc/logrotate.d/*

RUN dnf config-manager --enable epel --setopt="epel.priority=4"

# User
RUN useradd -m -u 1000 -g tape cta

# Bring in prebuilt RPMs
COPY ${LOCAL_RPM_DIR} ${CTA_REPO_DIR}/RPMS/x86_64

# Create local CTA repo
RUN createrepo "${CTA_REPO_DIR}" && \
    echo -e "[cta-local-testing]\n\
name=CTA repo with testing RPMs pointing to local artifacts\n\
baseurl=file://${CTA_REPO_DIR}\n\
gpgcheck=0\n\
enabled=1\n\
priority=2" > /etc/yum.repos.d/cta-local-testing.repo && \
    dnf install -y cta-release && \
    cta-versionlock apply

RUN dnf install -y \
  cta-debuginfo \
  cta-debugsource


###############################################
# SERVICE cta-taped
###############################################
FROM base AS cta-taped

# Ideally cta-tape-label should not be here
# Something for later..
RUN dnf install -y \
      mt-st \
      lsscsi \
      sg3_utils \
      cta-taped \
      cta-tape-label \
      cta-eosdf \
      cta-taped-debuginfo && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTA_REPO_DIR}" && dnf clean all --enablerepo=\*

# Can this run as non-root?
CMD ["/bin/bash", "-c", "runuser -c \"/usr/bin/cta-taped -c /etc/cta/cta-taped.conf --foreground --log-format=json --log-to-file=/var/log/cta/cta-taped.log\""]


###############################################
# SERVICE cta-rmcd
###############################################
FROM base AS cta-rmcd

RUN dnf install -y \
      mt-st \
      mtx \
      lsscsi \
      sg3_utils \
      cta-rmcd \
      cta-smc \
      cta-rmcd-debuginfo && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTA_REPO_DIR}" && dnf clean all --enablerepo=\*

# Can this can run as non-root?
CMD ["/usr/bin/cta-rmcd", "-f", "/dev/smc"]

###############################################
# SERVICE cta-maintd
###############################################
FROM base AS cta-maintd

RUN dnf install -y \
      cta-maintd \
      cta-maintd-debuginfo && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTA_REPO_DIR}" && dnf clean all --enablerepo=\*

USER cta
CMD ["/usr/bin/cta-maintd", "--foreground", "--log-to-file=/var/log/cta/cta-maintd.log", "--log-format json", "--config /etc/cta/cta-maintd.conf"]

###############################################
# SERVICE cta-frontend-grpc
###############################################
FROM base AS cta-frontend-grpc

RUN dnf install -y \
      cta-frontend-grpc \
      cta-frontend-grpc-debuginfo && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTA_REPO_DIR}" && dnf clean all --enablerepo=\*

USER cta
CMD ["/bin/bash", "-c", "/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend-grpc.log"]


###############################################
# SERVICE cta-frontend-xrd
###############################################
FROM base AS cta-frontend-xrd

RUN dnf install -y \
      cta-frontend \
      cta-frontend-debuginfo && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTA_REPO_DIR}" && dnf clean all --enablerepo=\*

USER cta
CMD ["/bin/bash", "-c", "cd ~cta; xrootd -l /var/log/cta-frontend-xrootd.log -k fifo -n cta -c /etc/cta/cta-frontend-xrootd.conf -I v4"]

###############################################
# TOOLS cta-tools-grpc
###############################################
FROM base AS cta-tools-grpc

RUN dnf install -y \
      cta-admin-grpc \
      cta-catalogueutils \
      cta-scheduler-utils && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTA_REPO_DIR}" && dnf clean all --enablerepo=\*

ENTRYPOINT ["/bin/bash"]

###############################################
# TOOLS cta-tools-xrd
###############################################
FROM base AS cta-tools-xrd

RUN dnf install -y \
      cta-cli \
      cta-catalogueutils \
      cta-scheduler-utils && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTA_REPO_DIR}" && dnf clean all --enablerepo=\*

ENTRYPOINT ["/bin/bash"]
