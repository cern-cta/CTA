# SPDX-FileCopyrightText: 2023 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


# CTA generic image containing all CTA RPMs
FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

ENV CTAREPODIR="/opt/repo"

COPY continuousintegration/docker/el9/etc/yum.repos.d-internal/* /etc/yum.repos.d-internal/

# Install necessary packages
RUN dnf install -y \
      python3-dnf-plugin-versionlock \
      dnf-utils \
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
    echo -e "[cta-local-testing]\n\
name=CTA repo with testing RPMs pointing to local artifacts\n\
baseurl=file://${CTAREPODIR}\n\
gpgcheck=0\n\
enabled=1\n\
priority=2" > /etc/yum.repos.d/cta-local-testing.repo && \
    dnf install -y "cta-release" && \
    cta-versionlock apply && \
    dnf clean all --enablerepo=\* && \
    rm -rf /etc/rc.d/rc.local

# Overwrite with internal repos if configured
ARG USE_INTERNAL_REPOS=FALSE
RUN if [ "${USE_INTERNAL_REPOS}" = "TRUE" ]; then \
      cp -f /etc/yum.repos.d-internal/* /etc/yum.repos.d/; \
    fi
