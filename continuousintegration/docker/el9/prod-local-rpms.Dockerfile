# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2023-2025 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest AS base

ENV CTAREPODIR="/opt/repo"

# Common repo configuration
COPY continuousintegration/docker/el9/etc/yum.repos.d-internal/* /etc/yum.repos.d/

# Core dependencies
RUN dnf install -y \
      python3-dnf-plugin-versionlock \
      dnf-utils \
      createrepo \
      epel-release \
      jq \
      krb5-workstation && \
    chmod 0644 /etc/logrotate.d/*

# User
RUN useradd -m -u 1000 -g tape cta

# Bring in prebuilt RPMs
COPY image_rpms ${CTAREPODIR}/RPMS/x86_64

# Create local CTA repo
RUN dnf config-manager --enable epel --setopt="epel.priority=4" && \
    createrepo "${CTAREPODIR}" && \
    echo -e "[cta-local-testing]\n\
name=CTA repo with testing RPMs pointing to local artifacts\n\
baseurl=file://${CTAREPODIR}\n\
gpgcheck=0\n\
enabled=1\n\
priority=2" > /etc/yum.repos.d/cta-local-testing.repo && \
    dnf install -y cta-release && \
    cta-versionlock apply && \
    dnf clean all --enablerepo=\* && \
    rm -rf /etc/rc.d/rc.local


###############################################
# SERVICE cta-frontend-grpc
###############################################
FROM base AS cta-frontend-grpc

RUN dnf install -y \
      cta-frontend-grpc \
      cta-debuginfo \
      cta-frontend-grpc-debuginfo \
      cta-debugsource && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTAREPODIR}"

USER cta
CMD ["/bin/bash", "-c", "/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend-grpc.log"]


###############################################
# SERVICE cta-frontend-xrd
###############################################
FROM base AS cta-frontend-xrd

RUN dnf install -y \
      cta-frontend \
      cta-debuginfo \
      cta-frontend-debuginfo \
      cta-debugsource && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTAREPODIR}"

USER cta
CMD ["/bin/bash", "-c", "cd ~cta; xrootd -l /var/log/cta-frontend-xrootd.log -k fifo -n cta -c /etc/cta/cta-frontend-xrootd.conf -I v4"]


###############################################
# SERVICE cta-taped
###############################################
FROM base AS cta-taped

RUN dnf install -y \
      mt-st \
      lsscsi \
      sg3_utils \
      cta-taped \
      cta-tape-label \
      cta-eosdf \
      cta-debuginfo \
      cta-taped-debuginfo \
      cta-debugsource && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTAREPODIR}"

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
      cta-debuginfo \
      cta-rmcd-debuginfo \
      cta-debugsource && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTAREPODIR}"

CMD ["/usr/bin/cta-rmcd", "-f", "/dev/smc"]

###############################################
# TOOLS cta-tools
###############################################
FROM base AS cta-tools

RUN dnf install -y \
      cta-catalogue-utils \
      cta-admin \
      cta-debuginfo \
      cta-objectstore-tools \
      cta-scheduler-utils \
      cta-cli \
      cta-admin-grpc \
      cta-debugsource && \
    dnf clean all

# Remove the local RPMs to reduce size
RUN rm -rf "${CTAREPODIR}"

ENTRYPOINT ["sleep", "infinity"]
