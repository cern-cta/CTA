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

# CTA generic image containing all CTA RPMs
FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

ENV CTAREPODIR="/opt/repo"

# Add orchestration run scripts locally
COPY continuousintegration/docker/opt /opt
COPY continuousintegration/docker/el9/etc/yum.repos.d-internal/* /etc/yum.repos.d-internal/

# Install necessary packages
RUN dnf install -y \
      python3-dnf-plugin-versionlock \
      yum-utils \
      createrepo \
      epel-release \
      jq \
      bc \
      sqlite \
      krb5-workstation && \
    # logrotate files must be 0644 or 0444
    chmod 0644 /etc/logrotate.d/*

# We add the cta user so that we can consistently reference it in the Helm chart when changing keytab ownership
RUN useradd -m -u 1000 -g tape cta

# Copy pre-built RPMs into the local repository directory
COPY image_rpms ${CTAREPODIR}/RPMS/x86_64

# Populate local repository and enable it
RUN dnf config-manager --enable epel --setopt="epel.priority=4" && \
    createrepo "${CTAREPODIR}" && \
    echo -e "[cta-artifacts]\n\
name=CTA artifacts\n\
baseurl=file://${CTAREPODIR}\n\
gpgcheck=0\n\
enabled=1\n\
priority=2" > /etc/yum.repos.d/cta-artifacts.repo && \
    dnf install -y "cta-release" && \
    cta-versionlock apply && \
    dnf clean all --enablerepo=\* && \
    rm -rf /etc/rc.d/rc.local

# Overwrite with internal repos if configured
ARG USE_INTERNAL_REPOS=FALSE
RUN if [ "${USE_INTERNAL_REPOS}" = "TRUE" ]; then \
      cp -f /etc/yum.repos.d-internal/* /etc/yum.repos.d/; \
    fi
