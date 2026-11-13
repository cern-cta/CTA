#!/bin/bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
set -euo pipefail

SUPPRESS_BUILD_SERVICE_STDOUT=${SUPPRESS_BUILD_SERVICE_STDOUT:-false}
ENABLE_ORACLE_SUPPORT=${ENABLE_ORACLE_SUPPORT:-false}
ENABLE_INTERNAL_REPOS=${ENABLE_INTERNAL_REPOS:-false}

if [[ "${SUPPRESS_BUILD_SERVICE_STDOUT}" == 1 || "${SUPPRESS_BUILD_SERVICE_STDOUT,,}" == true ]]; then
    exec 1>/dev/null
fi

if [[ "${ENABLE_ORACLE_SUPPORT}" == 1 || "${ENABLE_ORACLE_SUPPORT,,}" == true ]]; then
    echo 'Debian images do not support Oracle.' >&2
    exit 1
fi
if [[ "${ENABLE_INTERNAL_REPOS}" == 1 || "${ENABLE_INTERNAL_REPOS,,}" == true ]]; then
    echo 'Debian images use the public dependency repository only.' >&2
    exit 1
fi

export DEBIAN_FRONTEND=noninteractive
packages=("$@")
if [[ "${1:-}" == --debug ]]; then
    # Select actual package names, including optional dbgsym packages, without APT wildcard expansion.
    packages=(gdb)
    shopt -s nullglob
    for package in /mnt/packages/*.deb /mnt/packages/*.ddeb; do
        packages+=("$(dpkg-deb --field "$package" Package)")
    done
fi

# Refresh local metadata even for rebuilt packages with unchanged versions.
rm -f /var/lib/apt/lists/*_mnt_packages_*
apt-get update
apt-get install -y --no-install-recommends "${packages[@]}"

# Downloads and indexes live in cache mounts; remove only image-local build state.
rm -f /etc/apt/sources.list.d/cta-local.list /etc/apt/preferences.d/cta-local /tmp/cta-package-hash
rm -f /var/log/apt/* /var/log/dpkg.log
