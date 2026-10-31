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

# Split the package list while leaving RPM globs for DNF to expand.
read -r -a target_packages <<< "${1//$'\n'/ }"

# Install cta-release
# We install need to install regular dnf, because microdnf has no versionlocking functionality
base_packages=$(rpm -qa --qf '%{NAME}\n' | sort -u)
microdnf install -y cta-release dnf

# microdnf does not record dependency reasons for DNF; mark only newly added bootstrap packages as removable.
mapfile -t bootstrap_packages < <(comm -13 <(printf '%s\n' "$base_packages") <(rpm -qa --qf '%{NAME}\n' | sort -u))
if (( ${#bootstrap_packages[@]} )); then
    dnf mark remove "${bootstrap_packages[@]}"
fi
cta-versionlock apply

# Conditionally overwrite public repos
if [[ "$ENABLE_INTERNAL_REPOS" == "1" ]] || [[ "${ENABLE_INTERNAL_REPOS,,}" == "true" ]]; then
    cp -f /tmp/internal-repos/* /etc/yum.repos.d/
    # Track which repo files were added so that we can delete them later
    ls /tmp/internal-repos/ > /tmp/internal-repo-list.txt
fi

# Conditionally add Oracle support
if [[ "$ENABLE_ORACLE_SUPPORT" == "1" ]] || [[ "${ENABLE_ORACLE_SUPPORT,,}" == "true" ]]; then
    target_packages+=(cta-lib-catalogue-occi)
fi

# By default dnf looks at /etc/dnf for the versionlock; not /etc/yum
ln -sf /etc/yum/pluginconf.d/versionlock.list /etc/dnf/plugins/versionlock.list

# Install the target-specific packages
# Using dnf instead of microdnf! microdnf does not support versionlocking
dnf install -y --enablerepo crb "${target_packages[@]}"

# Keep requested packages and their dependencies when removing build-only tools.
dnf mark install "${target_packages[@]}"

# Allow DNF to remove itself and unused dependencies, including Python where it is not needed.
dnf remove -y --setopt=protected_packages= --setopt=clean_requirements_on_remove=True cta-release dnf

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
