#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Read the release family without accepting JSON numbers or silently using a fallback.
read_cta_release_family() {
  local family
  if ! family=$(jq -er '.releaseFamily | select(type == "string") | select(test("^[1-9][0-9]*$"))' "$1"); then
    echo "Invalid or missing releaseFamily in $1; expected a positive integer string." >&2
    return 1
  fi
  printf '%s\n' "$family"
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
  elif [[ "$version" =~ \.el[0-9]+$ ]]; then
    echo "CTA version platform does not match ${platform}: $1" >&2
    return 1
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

  # Variant and platform labels belong only at the end of the resolved version.
  if [[ ! "$version" =~ ^[0-9]+(\.[0-9]+)*-[a-z0-9]+([.][a-z0-9]+)*$ ]] \
      || [[ "$version" =~ \.(pgsched|pgcat|pgall|el[0-9]+)(\.|$) ]]; then
    echo "Invalid CTA version: $1; expected exactly one separating hyphen, as in 6-dev or 6.12.0.0-1." >&2
    return 1
  fi

  printf '%s%s.%s\n' "$version" "${variant:+.${variant}}" "$platform"
}
