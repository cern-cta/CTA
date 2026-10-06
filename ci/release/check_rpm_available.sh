#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

usage() {
  echo
  echo "Usage: $0 --repository-url <repo> --package <package> --version <version>"
  echo
  echo "Checks an exact package version-release in a given (dnf/yum) repo."
  echo "Use the full version including platform, without v (e.g. 6.12.0.0-1.pgall.el9)."
  echo
  exit 1
}

check_package_available() {

  local repository=""
  local package=""
  local version=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --repository-url)
        if [[ $# -gt 1 ]]; then
          repository="$2"
          shift
        else
          log_error "Error: --repository-url requires an argument"
          usage
        fi
        ;;
      --package)
        if [[ $# -gt 1 ]]; then
          package="$2"
          shift
        else
          log_error "Error: --package requires an argument"
          usage
        fi
        ;;
      --version)
        if [[ $# -gt 1 ]]; then
          version="$2"
          shift
        else
          log_error "Error: --version requires an argument"
          usage
        fi
        ;;
      *)
        echo "Invalid argument: $1"
        usage
        ;;
    esac
    shift
  done

  if [[ -z "${repository}" ]]; then
    echo "Failure: Missing mandatory argument --repository-url"
    usage
  fi
  if [[ -z "${package}" ]]; then
    echo "Failure: Missing mandatory argument --package"
    usage
  fi
  if [[ -z "${version}" ]]; then
    echo "Failure: Missing mandatory argument --version"
    usage
  fi

  if [[ ! "$version" =~ ^[0-9]+(\.[0-9]+)*-[a-z0-9]+([.][a-z0-9]+)*\.el[0-9]+$ ]]; then
    log_error "--version must include the platform, omit the leading v, and contain exactly one separating hyphen."
    exit 1
  fi

  echo "Checking whether $package version $version is available in the following repo:"
  echo "    $repository"

  tempdir=$(mktemp -d)
  trap 'rm -rf "$tempdir"' EXIT
  repofile="$tempdir/temp.repo"

  # Create a temporary .repo file pointing to the provided repo URL
  cat > "$repofile" <<EOF
[temp-repo]
name=Temporary Repo
baseurl=$repository
enabled=1
gpgcheck=0
EOF

  # Read repository metadata once and compare whole package/version fields.
  local available_packages
  if ! available_packages=$(dnf -q --repo=temp-repo --setopt=reposdir="$tempdir" \
      --refresh list --available --showduplicates "$package"); then
    log_error "Failed to query repository for package '$package'."
    exit 1
  fi
  echo "Available versions for package $package:"
  printf '%s\n' "$available_packages"

  if awk -v package="$package" -v version="$version" '
      $1 == package ".x86_64" {
        actual = $2
        sub(/^[0-9]+:/, "", actual)
        if (actual == version) found = 1
      }
      END { exit !found }
    ' <<< "$available_packages"; then
    echo "Package '$package' with version '$version' is available in the provided repository."
    exit 0
  else
    log_error "Failed: Package '$package' with version '$version' is NOT available in the provided repository."
    exit 1
  fi


}

check_package_available "$@"
