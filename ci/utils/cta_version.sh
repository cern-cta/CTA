#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Read the positive integer major version from project.json.
read_cta_major_version() {
  local major_version
  if ! major_version=$(jq -er '.majorVersion | numbers | select(. > 0 and . == floor)' "$1"); then
    echo "Invalid or missing majorVersion in $1; expected a positive integer." >&2
    return 1
  fi
  printf '%s\n' "$major_version"
}

# Require a complete build version without normalizing or rewriting the input.
validate_cta_version() {
  local version="$1"
  local platform_pattern='(el[0-9]+|(debian|ubuntu)[0-9]+([.][0-9]+)*)'

  if [[ ! "$version" =~ [.]${platform_pattern}$ ]]; then
    echo "Invalid CTA version: $1; expected a terminal platform suffix, such as .el9." >&2
    return 1
  fi
  version="${version%."${BASH_REMATCH[1]}"}"

  case "${version##*.}" in
    pgsched | pgcat | pgall) version="${version%.*}" ;;
  esac

  # Release image tags use neutral versions; branch builds retain their unique build ID.
  if [[ ! "$version" =~ ^[1-9][0-9]*\.[0-9]+\.[0-9]+(\.(dev|rc)[1-9][0-9]*)?$ ]] \
      && [[ ! "$version" =~ ^[1-9][0-9]*\.dev[1-9][0-9]*(\.g[a-f0-9]+(\.dirty)?)?$ ]]; then
    echo "Invalid CTA version: $1; expected major.minor.patch[.devN|.rcN][.variant].platform." >&2
    return 1
  fi
}

# Resolve a software version using the actual build variant and platform.
resolve_cta_version() {
  local version="${1#v}"
  local platform="$2"
  local scheduler="$3"
  local oracle="$4"
  local variant=""
  local supplied_variant=""

  case "${scheduler}:${oracle,,}" in
    objectstore:true | objectstore:on) ;;
    pgsched:true | pgsched:on) variant=pgsched ;;
    objectstore:false | objectstore:off) variant=pgcat ;;
    pgsched:false | pgsched:off) variant=pgall ;;
    *) echo "Invalid scheduler/Oracle configuration: ${scheduler}/${oracle}" >&2; return 1 ;;
  esac

  # Accept an already resolved version without adding its suffixes again.
  if [[ "$version" == *."$platform" ]]; then
    version="${version%."$platform"}"
  fi

  case "${version##*.}" in
    pgsched | pgcat | pgall)
      supplied_variant="${version##*.}"
      version="${version%.*}"
      if [[ "$supplied_variant" != "$variant" ]]; then
        echo "CTA version variant does not match the build configuration: $1" >&2
        return 1
      fi
      ;;
  esac

  version="${version}${variant:+.${variant}}.${platform}"
  validate_cta_version "$version" || return 1
  printf '%s\n' "$version"
}

# Convert an image version to native package metadata, keeping variants in the revision.
resolve_cta_package_version() {
  local version="$1"
  local platform_pattern='(el[0-9]+|(debian|ubuntu)[0-9]+([.][0-9]+)*)'
  local platform variant="" revision=1

  validate_cta_version "$version" || return 1
  [[ "$version" =~ [.]${platform_pattern}$ ]]
  platform="${BASH_REMATCH[1]}"
  version="${version%."$platform"}"
  case "${version##*.}" in
    pgsched | pgcat | pgall)
      variant="${version##*.}"
      version="${version%.*}"
      ;;
  esac

  # Prerelease status belongs to the upstream version for both package formats.
  version="${version/.dev/~dev}"
  version="${version/.rc/~rc}"

  case "$platform" in
    el*) printf '%s-%s%s.%s\n' "$version" "$revision" "${variant:+.$variant}" "$platform" ;;
    debian* | ubuntu*) printf '%s-%s%s\n' "$version" "$revision" "${variant:+.$variant}" ;;
  esac
}
