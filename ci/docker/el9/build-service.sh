#!/bin/bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
set -e

# Script for installing an arbitrary number of RPMs packages through (micro)dnf while also cleaning up everything nicely.
# While all of this logic could be executed directly in the Dockerfile, it causes a significant amount of code duplication
# It is important that everything runs in a single stage to ensure we don't bloat the layer sizes

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
    microdnf install -y cta-catalogue-occi
fi

# Install the target-specific packages
microdnf install -y --enablerepo crb $TARGET_PACKAGES

# Cleanup to reduce image size
microdnf remove -y cta-release python*
rpm -q systemd-* > /dev/null && rpm -e systemd-* --nodeps || true
rm -rf /var/lib/dnf/history.* /tmp/internal-repos
# Do not "microdnf clean all", because /var/yum is mounted as a cache, so that would not affect final image size
# and would actually clear the cache which we don't want
