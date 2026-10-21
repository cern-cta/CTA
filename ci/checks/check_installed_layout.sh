#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Smoke-test CTA's installed layout and package ownership, not full packaging-policy compliance.
# Exact CTA filenames are project choices within these conventions.

set -euo pipefail

# Account for RPM lib64 and Debian multiarch library directories.
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/RPMMacros/
# Debian: https://www.debian.org/doc/debian-policy/ch-opersys.html#file-system-structure
case "${1:-}" in
  rpm)
    libdir=/usr/lib64
    mapfile -t packages < <(rpm -qa --qf '%{NAME}\n' | grep '^cta-')
    files=$(rpm -ql "${packages[@]}")
    configs=$(rpm -q --qf '[%{FILENAMES} %{FILEFLAGS:fflags}\n]' "${packages[@]}" | \
      awk '$2 ~ /c/ && $2 ~ /n/ {print $1}')
    ;;
  deb)
    libdir=/usr/lib/x86_64-linux-gnu
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
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/#_file_ownership
# Permissions: https://www.debian.org/doc/debian-policy/ch-files.html#permissions-and-owners
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/#_file_permissions
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
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/#_configuration_files
# Debian: https://www.debian.org/doc/debian-policy/ch-files.html#configuration-files
require_config() {
  require_file "$1"
  grep -Fxq -- "$1" <<< "$configs" || fail "missing conffile or config(noreplace) metadata: $1"
}

# Debian documents gzip compression for installed manuals and documentation.
# https://www.debian.org/doc/debian-policy/ch-docs.html
require_document() {
  local path=$1
  # Package tooling may compress manuals and examples.
  if [[ -f "$path.gz" ]]; then
    require_file "$path.gz"
  else
    require_file "$path"
  fi
}

# Accept user-command or system-administration manuals, including compressed pages.
require_manual() {
  local tool=$1 section path
  for section in 1 8; do
    path="/usr/share/man/man$section/$tool.$section"
    if [[ -f "$path" || -f "$path.gz" ]]; then
      require_document "$path"
      return
    fi
  done
  fail "missing section 1 or 8 manual for $tool"
}

# Commands must be executable and ship manuals.
# FHS binaries: https://refspecs.linuxfoundation.org/FHS_3.0/fhs/ch04s04.html
# Manuals: https://www.debian.org/doc/debian-policy/ch-docs.html#manual-pages
for tool in cta-admin cta-catalogue-schema-create cta-readtp cta-tape-label cta-smc cta-eos-restore-files \
  cta-taped cta-maintd cta-rmcd cta-frontend-grpc; do
  require_file "/usr/bin/$tool"
  [[ -x "/usr/bin/$tool" ]] || fail "not executable: /usr/bin/$tool"
  require_manual "$tool"
done

# Services ship vendor units under /usr/lib/systemd/system.
# TODO: Add frontend and taped service checks once they migrate to the runtime library.
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/Systemd/
for service in cta-maintd cta-rmcd; do
  require_file "/usr/lib/systemd/system/$service.service"
done

# Administrator-editable log rotation settings belong under /etc.
# Configuration: https://www.debian.org/doc/debian-policy/ch-files.html#location
# Log rotation: https://www.debian.org/doc/debian-policy/ch-opersys.html#log-files
for service in cta-maintd cta-rmcd; do
  require_config "/etc/logrotate.d/$service"
done

# Examples belong under the owning package's documentation directory.
# Debian: https://www.debian.org/doc/debian-policy/ch-docs.html#examples
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/#_documentation
examples=(
  cta-taped/examples/cta-taped.example.conf
  cta-maintd/examples/cta-maintd.example.toml
  cta-rmcd/examples/cta-rmcd.example.toml
  cta-frontend-grpc/examples/cta-frontend.example.conf
  cta-frontend-grpc/examples/cta-frontend-grpc.example.toml
  cta-frontend-grpc/examples/jwt-revoke-list.example.toml
)
case "$1" in
  rpm)
    examples+=(
      cta-admin/examples/cta-admin.example.conf
      cta-lib-catalogue/examples/cta-catalogue.example.conf
      cta-lib-scheduler/examples/cta-scheduler.example.conf
      cta-lib-telemetry/examples/cta-otel.example.yaml
    )
    ;;
  deb)
    examples+=(
      cta-tools/examples/cta-admin.example.conf
      cta-runtime/examples/cta-catalogue.example.conf
      cta-runtime/examples/cta-scheduler.example.conf
      cta-runtime/examples/cta-otel.example.yaml
    )
    ;;
esac
for example in "${examples[@]}"; do
  require_document "/usr/share/doc/$example"
done

# Vendor account definitions use the sysusers.d discovery path.
# https://www.freedesktop.org/software/systemd/man/latest/sysusers.d.html
require_file /usr/lib/sysusers.d/cta.conf
# Architecture-independent runtime data belongs under /usr/share.
# https://www.debian.org/doc/debian-policy/ch-opersys.html#file-system-structure
require_file /usr/share/cta/cta-logging.schema.json
# Each shell has its own completion discovery directory.
# https://docs.fedoraproject.org/en-US/packaging-guidelines/ShellCompletions/
require_file /usr/share/bash-completion/completions/cta-eos-restore-files
require_file /usr/share/zsh/site-functions/_cta-eos-restore-files
require_file /usr/share/fish/vendor_completions.d/cta-eos-restore-files.fish

# Check versioned runtime libraries without hardcoding the current ABI version.
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/#_shared_libraries
# Debian: https://www.debian.org/doc/debian-policy/ch-sharedlibs.html#run-time-shared-libraries
for library in libctacommon libctacatalogue libctascheduler; do
  mapfile -t paths < <(grep -E "^${libdir}/${library}\.so\.[0-9]" <<< "$files")
  ((${#paths[@]})) || fail "missing versioned $library in $libdir"
  for path in "${paths[@]}"; do
    require_file "$path"
  done
done

# Reserve /usr/local for administrators.
# Inspect all packaged paths, even when a correctly installed copy also exists.
# /usr/local: https://www.debian.org/doc/debian-policy/ch-opersys.html#site-specific-programs
while IFS= read -r path; do
  case "$path" in
    /usr/local/*) fail "package installs into /usr/local: $path" ;;
  esac
done <<< "$files"

echo "Installed CTA layout checks passed (${1})."
