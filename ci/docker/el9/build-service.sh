#!/bin/bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
set -e

# Script for installing an arbitrary number of RPMs packages through (micro)dnf while also cleaning up everything nicely.
# While all of this logic could be executed directly in the Dockerfile, it causes a significant amount of code duplication and
# it is important that everything runs in a single layer to ensure we don't bloat the image sizes

# Since we are building in parallel, suppress stdout to reduce noise and make errors easier to spot
exec 1> /dev/null

TARGET_PACKAGES=$1

# Install cta-release
microdnf install -y cta-release
cta-versionlock apply

# Conditionally overwrite public repos
if [ "$USE_INTERNAL_REPOS" = "1" ]; then
    cp -f /tmp/internal-repos/* /etc/yum.repos.d/
fi

# Conditionally add Oracle support
if [ "$USE_ORACLE_CATALOGUE" = "1" ]; then
    TARGET_PACKAGES="$TARGET_PACKAGES cta-lib-catalogue-occi"
fi

# Install the target-specific packages
microdnf install -y --enablerepo crb $TARGET_PACKAGES

# Cleanup to reduce image size
# cta-release brings in Python, but uninstalling it for some reason does not remove it
# Nothing in CTA requires python and it adds a lot to the final image size, so we remove it here explicitly
microdnf remove -y cta-release python*
# Remove systemd stuff because we don't rely on systemd in containers
rpm -q systemd-* > /dev/null && rpm -e systemd-* --nodeps || true
# Clean up history and internal repos
rm -rf /var/lib/dnf/history.* /tmp/internal-repos
# Do not "microdnf clean all", because /var/yum is mounted as a cache, so that would not affect final image size
# and would actually clear the cache which we don't want
