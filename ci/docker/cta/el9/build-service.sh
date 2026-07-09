#!/bin/bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
set -eo pipefail

# Script for installing an arbitrary number of RPMs packages through (micro)dnf while also cleaning up everything nicely.
# While all of this logic could be executed directly in the Dockerfile, it causes a significant amount of code duplication and
# it is important that everything runs in a single layer to ensure we don't bloat the image sizes

# Since we are building in parallel, suppress stdout to reduce noise and make errors easier to spot
exec 1> /dev/null

TARGET_PACKAGES=$1

# Install cta-release
# We install need to install regular dnf, because microdnf has no versionlocking functionality
microdnf install -y cta-release dnf
cta-versionlock apply

# Conditionally overwrite public repos
if [ "$USE_INTERNAL_REPOS" = "1" ]; then
    cp -f /tmp/internal-repos/* /etc/yum.repos.d/
    # Track which repo files were added so that we can delete them later
    ls /tmp/internal-repos/ > /tmp/internal-repo-list.txt
fi

# Conditionally add Oracle support
if [ "$USE_ORACLE_CATALOGUE" = "1" ]; then
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

if [ "$USE_INTERNAL_REPOS" = "1" ]; then
    while IFS= read -r filename; do
        if [ -n "$filename" ]; then
            rm -f "/etc/yum.repos.d/$filename"
        fi
    done < /tmp/internal-repo-list.txt
    rm -f /tmp/internal-repo-list.txt
fi

# Remove systemd stuff because we don't rely on systemd in containers
# Ideally the base image doesn't contain systemd in the first place, but the layer in which we install the majority of the packages still pull in quite some systemd specific stuff
rpm -q systemd > /dev/null && rpm -e systemd systemd-* --nodeps || true
# Clean up history and internal repos
rm -rf /var/lib/dnf/history.* /tmp/internal-repos /etc/yum.repos.d/cta.repo
# Do not "microdnf clean all", because /var/yum is mounted as a cache, so that would not affect final image size
# and would actually clear the cache which we don't want
