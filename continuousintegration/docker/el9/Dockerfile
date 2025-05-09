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

# Add orchestration run scripts locally
COPY continuousintegration/docker/opt /opt

ARG USE_INTERNAL_REPOS=0

# Ensure we have access to our own internal repos and the local artifact CTA RPMs
COPY continuousintegration/docker/el9/etc/yum.repos.d-public/ /etc/yum.repos.d/
COPY continuousintegration/docker/el9/etc/yum.repos.d/ /tmp/
RUN if [ "$USE_INTERNAL_REPOS" = "1" ]; then \
      mv -f /tmp/*.repo /etc/yum.repos.d/; \
    fi
# Copy pre-built RPMs into the local repository directory
COPY image_rpms /opt/repo/RPMS/x86_64

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

# Populate local repository and enable it
RUN dnf config-manager --enable epel --setopt="epel.priority=4" && \
    createrepo "/opt/repo" && \
    dnf config-manager --enable cta-artifacts && \
    dnf install -y "cta-release" && \
    cta-versionlock apply && \
    dnf clean all --enablerepo=\* && \
    rm -rf /etc/rc.d/rc.local
