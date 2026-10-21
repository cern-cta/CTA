#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Smoke-test CTA's installed layout and package ownership, not full packaging-policy compliance.
# Debian Policy: filesystem hierarchy (9.1), configuration files (10.7), documentation (12).
# https://www.debian.org/doc/debian-policy/ch-opersys.html#file-system-hierarchy
# https://www.debian.org/doc/debian-policy/ch-files.html#configuration-files
# https://www.debian.org/doc/debian-policy/ch-docs.html

set -euo pipefail

# Account for RPM lib64/sysconfig and Debian multiarch/default conventions.
case "${1:-}" in
  rpm)
    libdir=/usr/lib64
    defaults=/etc/sysconfig
    mapfile -t packages < <(rpm -qa --qf '%{NAME}\n' | grep '^cta-')
    files=$(rpm -ql "${packages[@]}")
    configs=$(rpm -q --qf '[%{FILENAMES} %{FILEFLAGS:fflags}\n]' "${packages[@]}" | \
      awk '$2 ~ /c/ && $2 ~ /n/ {print $1}')
    ;;
  deb)
    libdir=/usr/lib/x86_64-linux-gnu
    defaults=/etc/default
    mapfile -t packages < <(dpkg-query -W -f='${Package} ${Status}\n' | \
      awk '$1 ~ /^(cta($|-)|libcta)/ && $2 " " $3 " " $4 == "install ok installed" {print $1}')
    files=$(dpkg-query -L "${packages[@]}")
    configs=$(dpkg-query -W -f='${Conffiles}\n' "${packages[@]}" | awk 'NF == 2 {print $1}')
    ;;
  *) echo "Usage: $0 rpm|deb" >&2; exit 2 ;;
esac

fail() {
  echo "Installed layout: $*" >&2
  exit 1
}

# Require both an installed file and package ownership at the expected path.
require_file() {
  local path=$1 owner mode
  [[ -f "$path" ]] || fail "missing $path"
  grep -Fxq -- "$path" <<< "$files" || fail "not owned by a CTA package: $path"

  # Follow library symlinks; root:tape ownership is valid, but writable group/other bits are not.
  read -r owner mode < <(stat -L -c '%u %a' -- "$path")
  [[ "$owner" == 0 ]] || fail "not root-owned: $path (uid $owner)"
  (( (8#$mode & 0022) == 0 )) || fail "group/world-writable: $path (mode $mode)"
}

# Package metadata must preserve administrator edits on upgrade.
require_config() {
  require_file "$1"
  grep -Fxq -- "$1" <<< "$configs" || fail "missing conffile or config(noreplace) metadata: $1"
}

require_document() {
  local path=$1
  # Package tooling may compress manuals and examples.
  if [[ -f "$path.gz" ]]; then
    require_file "$path.gz"
  else
    require_file "$path"
  fi
}

# Representative commands must be executable and ship section-1 manuals.
for tool in cta-admin cta-catalogue-schema-create cta-readtp cta-tape-label cta-smc cta-eos-restore-files; do
  require_file "/usr/bin/$tool"
  [[ -x "/usr/bin/$tool" ]] || fail "not executable: /usr/bin/$tool"
  require_document "/usr/share/man/man1/$tool.1"
done

# Services ship vendor units under /usr/lib/systemd/system, with their binaries and manuals.
for service in cta-taped cta-maintd cta-rmcd cta-frontend-grpc; do
  require_file "/usr/bin/$service"
  [[ -x "/usr/bin/$service" ]] || fail "not executable: /usr/bin/$service"
  require_file "/usr/lib/systemd/system/$service.service"
  require_document "/usr/share/man/man1/$service.1"
done

# Administrator-editable log rotation and service settings belong under /etc.
for service in cta-taped cta-maintd cta-rmcd; do
  require_config "/etc/logrotate.d/$service"
done
for service in cta-taped cta-frontend-grpc; do
  require_config "$defaults/$service"
done

# CTA groups example configurations under documentation, separate from active configuration.
for example in cta-admin.example.conf cta-catalogue.example.conf cta-scheduler.example.conf \
  cta-taped.example.conf cta-maintd.example.toml cta-rmcd.example.toml \
  cta-frontend.example.conf cta-frontend-grpc.example.toml jwt-revoke-list.example.toml cta-otel.example.yaml; do
  require_document "/usr/share/doc/cta/examples/$example"
done

# Account definitions, runtime data and shell completions use their standard discovery paths.
require_file /usr/lib/sysusers.d/cta.conf
require_file /usr/share/cta/cta-logging.schema.json
require_file /usr/share/bash-completion/completions/cta-eos-restore-files
require_file /usr/share/zsh/site-functions/_cta-eos-restore-files
require_file /usr/share/fish/vendor_completions.d/cta-eos-restore-files.fish

# Check versioned runtime libraries without hardcoding the current ABI version.
for library in libctacommon libctacatalogue libctascheduler; do
  mapfile -t paths < <(grep -E "^${libdir}/${library}\.so\.[0-9]" <<< "$files")
  ((${#paths[@]})) || fail "missing versioned $library in $libdir"
  for path in "${paths[@]}"; do
    require_file "$path"
  done
done

# Reserve /usr/local for administrators and reject misplaced examples or obsolete manual sections.
# Inspect all packaged paths, even when a correctly installed copy also exists.
while IFS= read -r path; do
  case "$path" in
    /usr/local/*) fail "package installs into /usr/local: $path" ;;
    *.example.conf*|*.example.toml*|*.example.yaml*)
      [[ "$path" == /usr/share/doc/cta/examples/* ]] || fail "misplaced example: $path"
      ;;
    /usr/share/man/*)
      [[ "$path" != *.1cta* ]] || fail "nonstandard manual section: $path"
      ;;
  esac
done <<< "$files"

echo "Installed CTA layout checks passed (${1})."
