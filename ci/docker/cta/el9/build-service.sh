#!/bin/bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
set -eo pipefail

# Script for installing an arbitrary number of RPMs packages through (micro)dnf while also cleaning up everything nicely.
# While all of this logic could be executed directly in the Dockerfile, it causes a significant amount of code duplication and
# it is important that everything runs in a single layer to ensure we don't bloat the image sizes

# Package-manager output is useful when diagnosing slow or failed image builds.
# Consumers that prefer quieter output can explicitly suppress it.
if [[ "${SUPPRESS_BUILD_SERVICE_STDOUT:-false}" == "1" ]] || \
    [[ "${SUPPRESS_BUILD_SERVICE_STDOUT,,}" == "true" ]]; then
    exec 1> /dev/null
fi

TARGET_PACKAGES=$1

# Install cta-release
# We install need to install regular dnf, because microdnf has no versionlocking functionality
microdnf install -y cta-release dnf
cta-versionlock apply

# Conditionally overwrite public repos
if [[ "$ENABLE_INTERNAL_REPOS" == "1" ]] || [[ "${ENABLE_INTERNAL_REPOS,,}" == "true" ]]; then
    cp -f /tmp/internal-repos/* /etc/yum.repos.d/
    # Track which repo files were added so that we can delete them later
    ls /tmp/internal-repos/ > /tmp/internal-repo-list.txt
fi

# Conditionally add Oracle support
if [[ "$ENABLE_ORACLE_SUPPORT" == "1" ]] || [[ "${ENABLE_ORACLE_SUPPORT,,}" == "true" ]]; then
    TARGET_PACKAGES="$TARGET_PACKAGES cta-lib-catalogue-occi"
fi

# By default dnf looks at /etc/dnf for the versionlock; not /etc/yum
ln -sf /etc/yum/pluginconf.d/versionlock.list /etc/dnf/plugins/versionlock.list

# Install the target-specific packages
# Using dnf instead of microdnf! microdnf does not support versionlocking
dnf install -y --enablerepo crb $TARGET_PACKAGES

# Cleanup to reduce image size
# cta-release brings in Python, but uninstalling it for some reason does not remove it
microdnf remove -y cta-release
rpm -e dnf # dnf is protected; microdnf does not want to delete it

# cta-release pulls in python, but microdnf does not autoremove it when uninstalling cta-release.
# Nothing in CTA requires python and it adds a lot to the final image size, so we remove it here explicitly
# Future improvement: handle this gracefully. Basically we try to remove python but if there are packages requiring it, we don't
# It produces some potentially misleading error messages though
microdnf remove -y python* > /dev/null || true

if [[ "$ENABLE_INTERNAL_REPOS" == "1" ]] || [[ "${ENABLE_INTERNAL_REPOS,,}" == "true" ]]; then
    while IFS= read -r filename; do
        if [[ -n "$filename" ]]; then
            rm -f "/etc/yum.repos.d/$filename"
        fi
    done < /tmp/internal-repo-list.txt
    rm -f /tmp/internal-repo-list.txt
fi

# Clean up history and internal repos
rm -rf /var/lib/dnf/history.* /tmp/internal-repos /etc/yum.repos.d/cta.repo
# Do not "microdnf clean all", because /var/yum is mounted as a cache, so that would not affect final image size
# and would actually clear the cache which we don't want
