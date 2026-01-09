# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# CTA generic image containing all CTA RPMs
# This image is similar to the one created by Dockerfile, except that this uses RPMs for a remote repo instead of local ones
# As a result, any installs on this image will also pull from public yum repos instead of private ones
FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

COPY ci/docker/el9/etc/yum.repos.d-internal/* /etc/yum.repos.d-internal/
# The CTA repo is a special case as it provides CTA itself. As such, we cannot put it in
# yum.repos.d-public, as this might overwrite an existing CTA repo when installing cta-release
# If we were to put it in yum.repos.d-internal, we would have to manually copy it from internal
# even if we want the public repos, which is not nice.
COPY ci/docker/el9/cta-public-testing.repo /etc/yum.repos.d/

# Install necessary packages
RUN dnf install -y \
      python3-dnf-plugin-versionlock \
      dnf-utils \
      epel-release \
      jq \
      bc \
      krb5-workstation && \
    # logrotate files must be 0644 or 0444
    chmod 0644 /etc/logrotate.d/*

# We add the cta user so that we can consistently reference it in the Helm chart when changing keytab ownership
RUN useradd -m -u 1000 -g tape cta

# Variable to specify the version to be used for CTA RPMs from the cta-ci-repo
# Format: X.YY.ZZ.A-B
ARG PUBLIC_REPO_VER

# Install cta-release and clean up
RUN dnf config-manager --enable epel --setopt="epel.priority=4" && \
    dnf install -y "cta-release-${PUBLIC_REPO_VER}" && \
    cta-versionlock apply && \
    dnf clean all --enablerepo=\* && \
    rm -rf /etc/rc.d/rc.local

# Overwrite with internal repos if configured
ARG USE_INTERNAL_REPOS=FALSE
RUN if [ "${USE_INTERNAL_REPOS}" = "TRUE" ]; then \
      cp -f /etc/yum.repos.d-internal/* /etc/yum.repos.d/; \
    fi
