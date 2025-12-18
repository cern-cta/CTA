#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

usage() {
  echo
  echo "Usage: $0 --repository-url <repo> --package <package> --version <version>"
  echo
  echo "Checks whether a package of a given version is available in a given (dnf/yum) repo."
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
          echo "Error: --repository-url requires an argument"
          usage
        fi
        ;;
      --package)
        if [[ $# -gt 1 ]]; then
          package="$2"
          shift
        else
          echo "Error: --package requires an argument"
          usage
        fi
        ;;
      --version)
        if [[ $# -gt 1 ]]; then
          version="$2"
          shift
        else
          echo "Error: --version requires an argument"
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

  # Check version available using dnf
  echo "Available versions for package $package:"
  dnf --repo=temp-repo --setopt=reposdir="$tempdir" list --showduplicates "$package"

  if dnf --repo=temp-repo --setopt=reposdir="$tempdir" list --showduplicates "$package" | grep -q "$version"; then
    echo "Package '$package' with version '$version' is available in the provided repository."
    exit 0
  else
    echo "Failed: Package '$package' with version '$version' is NOT available in the provided repository."
    exit 1
  fi


}

check_package_available "$@"
