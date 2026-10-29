#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Smoke-test CTA's installed layout and package ownership, not full packaging-policy compliance.
# Exact CTA filenames are project choices within these conventions.

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

package_format=${1:-}

# Add tools here to check their binaries and manuals.
tools=(
  cta-admin
  cta-catalogue-schema-create
  cta-readtp
  cta-tape-label
  cta-smc
)

# Add Rust tools here to check their binaries, manuals and all shell completions.
rust_tools=(
  cta-eos-restore-files
)

# Add runtime-library services here to check binaries, manuals, units, log rotation and TOML examples.
services=(
  cta-maintd
  cta-rmcd
)

# Account for RPM lib64 and Debian multiarch library directories.
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/RPMMacros/
# Debian: https://www.debian.org/doc/debian-policy/ch-opersys.html#file-system-structure
case "$package_format" in
  rpm)
    libdir=/usr/lib64
    mapfile -t packages < <(rpm -qa --qf '%{NAME}\n' | grep -E '^(cta-|ctaops-udev$)')
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
  *) log_error "Usage: $0 rpm|deb"; exit 2 ;;
esac

fail() {
  log_error "ERROR: CTA package layout: $*"
  exit 1
}

# Require both an installed file and package ownership at the expected path.
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/#_file_ownership
# Permissions: https://www.debian.org/doc/debian-policy/ch-files.html#permissions-and-owners
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/#_file_permissions
require_file() {
  local path=$1 package=${2:-} owner mode package_files
  [[ -f "$path" ]] || fail "missing $path"
  grep -Fxq -- "$path" <<< "$files" || fail "not owned by a CTA package: $path"
  if [[ -n "$package" ]]; then
    case "$package_format" in
      rpm) package_files=$(rpm -ql "$package") ;;
      deb) package_files=$(dpkg-query -L "$package") ;;
    esac
    grep -Fxq -- "$path" <<< "$package_files" || fail "not owned by $package: $path"
  fi

  # Follow library symlinks; root:tape ownership is valid, but writable group/other bits are not.
  read -r owner mode < <(stat -L -c '%u %a' -- "$path")
  [[ "$owner" == 0 ]] || fail "not root-owned: $path (uid $owner)"
  (( (8#$mode & 0022) == 0 )) || fail "group/world-writable: $path (mode $mode)"
}

# Package metadata must preserve administrator edits on upgrade.
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/#_configuration_files
# Debian: https://www.debian.org/doc/debian-policy/ch-files.html#configuration-files
require_config() {
  require_file "$1" "${2:-}"
  if ! grep -Fxq -- "$1" <<< "$configs"; then
    case "$package_format" in
      rpm) fail "missing RPM %config(noreplace) flags: $1" ;;
      deb) fail "not registered as a Debian conffile: $1" ;;
    esac
  fi
}

# Debian documents gzip compression for installed manuals and documentation.
# https://www.debian.org/doc/debian-policy/ch-docs.html
require_document() {
  local path=$1 package=${2:-}
  # Package tooling may compress manuals and examples.
  if [[ -f "$path.gz" ]]; then
    require_file "$path.gz" "$package"
  else
    require_file "$path" "$package"
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

log_task "Checking commands and manuals ($package_format)..."

# Commands must be executable and ship manuals.
# FHS binaries: https://refspecs.linuxfoundation.org/FHS_3.0/fhs/ch04s04.html
# Manuals: https://www.debian.org/doc/debian-policy/ch-docs.html#manual-pages
for tool in "${tools[@]}" "${rust_tools[@]}" "${services[@]}"; do
  require_file "/usr/bin/$tool"
  [[ -x "/usr/bin/$tool" ]] || fail "not executable: /usr/bin/$tool"
  require_manual "$tool"
done

# The udev helper is currently packaged only for RPM, outside /usr/bin.
if [[ "$package_format" == rpm ]]; then
  log_task "Checking CTA udev rules and helper..."
  require_file /usr/lib/udev/ctaops-udev ctaops-udev
  [[ -x /usr/lib/udev/ctaops-udev ]] || fail "not executable: /usr/lib/udev/ctaops-udev"
  require_file /usr/lib/udev/rules.d/62-ctaops.rules ctaops-udev
  require_config /usr/lib/udev/ctaops-udev.ini ctaops-udev
  require_document /usr/share/man/man8/ctaops-udev.8ctaops ctaops-udev
fi

log_task "Checking service units and configuration ($package_format)..."

# Services ship vendor units under /usr/lib/systemd/system.
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/Systemd/
for service in "${services[@]}"; do
  require_file "/usr/lib/systemd/system/$service.service"
done

# Administrator-editable log rotation settings belong under /etc.
# Configuration: https://www.debian.org/doc/debian-policy/ch-files.html#location
# Log rotation: https://www.debian.org/doc/debian-policy/ch-opersys.html#log-files
for service in "${services[@]}"; do
  require_config "/etc/logrotate.d/$service"
done

log_task "Checking examples and their package ownership ($package_format)..."

# Examples belong under the owning package's documentation directory.
# Debian: https://www.debian.org/doc/debian-policy/ch-docs.html#examples
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/#_documentation
examples=(
  cta-admin:cta-admin.example.conf
  cta-lib-catalogue:cta-catalogue.example.conf
  cta-lib-scheduler:cta-scheduler.example.conf
  cta-lib-telemetry:cta-otel.example.yaml
)
for example in "${examples[@]}"; do
  package=${example%%:*}
  require_document "/usr/share/doc/$package/examples/${example#*:}" "$package"
done
for service in "${services[@]}"; do
  require_document "/usr/share/doc/$service/examples/$service.example.toml" "$service"
done

log_task "Checking copyright and license files ($package_format)..."

# Every installed CTA package provides its copyright/license file.
# Debian: https://www.debian.org/doc/debian-policy/ch-docs.html#copyright-information
# Fedora: https://docs.fedoraproject.org/en-US/packaging-guidelines/LicensingGuidelines/#_license_text
# Follow documentation symlinks, including Debian debug packages sharing their parent's files.
for package in "${packages[@]}"; do
  case "$package_format" in
    rpm) license_file="/usr/share/licenses/$package/COPYING" ;;
    deb) license_file="/usr/share/doc/$package/copyright" ;;
  esac
  [[ -f "$license_file" && -r "$license_file" ]] || fail "missing or unreadable license file: $license_file"
done

log_task "Checking runtime data and shell completions ($package_format)..."

# Vendor account definitions use the sysusers.d discovery path.
# https://www.freedesktop.org/software/systemd/man/latest/sysusers.d.html
require_file /usr/lib/sysusers.d/cta.conf
# Architecture-independent runtime data belongs under /usr/share.
# https://www.debian.org/doc/debian-policy/ch-opersys.html#file-system-structure
require_file /usr/share/cta/cta-logging.schema.json
# Each shell has its own completion discovery directory.
# https://docs.fedoraproject.org/en-US/packaging-guidelines/ShellCompletions/
for tool in "${rust_tools[@]}"; do
  require_file "/usr/share/bash-completion/completions/$tool"
  require_file "/usr/share/zsh/site-functions/_$tool"
  require_file "/usr/share/fish/vendor_completions.d/$tool.fish"
done

log_task "Checking runtime libraries ($package_format)..."

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

log_task "Checking for files under /usr/local ($package_format)..."

# Reserve /usr/local for administrators.
# Inspect all packaged paths, even when a correctly installed copy also exists.
# /usr/local: https://www.debian.org/doc/debian-policy/ch-opersys.html#site-specific-programs
if misplaced=$(grep '^/usr/local/' <<< "$files"); then
  fail "package installs into /usr/local: $misplaced"
fi

log_success "SUCCESS: Installed CTA layout checks passed ($package_format)."
