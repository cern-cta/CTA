#!/bin/bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
set -euo pipefail

# Run inside the image as its configured user, after building from real CTA packages.
target=${1:?Expected a CTA image target}
test "$(dpkg --print-architecture)" = amd64
test "$(getent passwd cta | cut -d: -f3,4)" = 1000:1000
test ! -e /etc/apt/sources.list.d/cta-local.list
test ! -e /tmp/cta-package-hash

if [[ "$target" != cta-debug ]]; then
    installed=$(dpkg-query -W -f='${binary:Package} ${db:Status-Status}\n' | awk '$2 == "installed" {print $1}')
    if grep -Eq '^(cta-tests|libcta-dev|.*-dbgsym|build-essential|dpkg-dev|gdb|systemd|logrotate)(:.*)?$' <<< "$installed"; then
        echo 'Unexpected build, debug, test or recommended service packages in runtime image' >&2
        exit 1
    fi
fi

case "$target" in
    cta-taped|cta-rmcd|cta-maintd|cta-admin-api|cta-workflow-api)
        test "$(id -u)" = 1000
        test "$(id -g)" = 1000
        test -w /run/cta
        test -x "/usr/bin/$target"
        libraries=$(ldd "/usr/bin/$target")
        if grep -q 'not found' <<< "$libraries"; then
            printf '%s\n' "$libraries" >&2
            exit 1
        fi
        ;;
    cta-tools) test -x /usr/bin/cta-admin ;;
    cta-debug) test -x /usr/bin/gdb ;;
    *) echo "Unknown image target: $target" >&2; exit 1 ;;
esac

case "$target" in
    cta-taped|cta-rmcd)
        test ! -e /usr/bin/cta-smc
        test ! -e /usr/bin/cta-readtp
        ;;
esac
if [[ "$target" == cta-taped ]]; then
    test -x /usr/bin/cta-eosdf.sh
    command -v xrdfs
    test "$(dpkg-query -S /usr/bin/cta-eosdf.sh)" = 'cta-eosdf: /usr/bin/cta-eosdf.sh'
    getcap /usr/bin/cta-taped | grep -q 'cap_sys_rawio=ep'
fi
case "$target" in
    cta-admin-api|cta-workflow-api|cta-tools) /usr/local/bin/grpc-health-probe -version ;;
esac
