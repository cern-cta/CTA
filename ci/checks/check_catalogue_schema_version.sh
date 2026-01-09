#!/bin/bash

# SPDX-FileCopyrightText: 2023 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

usage() {
  echo
  echo "Usage: $0 [options] <cta_repo_dir>"
  echo
  echo "Validates the CTA repository."
  echo "  <cta_repo_dir>:          Path to the CTA repository."
  echo
  echo "options:"
  echo "  -p:                      Validate pivot release."
  echo "  -n:                      Validate non-pivot release."
  echo "  -t:                      Validate CTA catalogue schema submodule tag."
  echo
  exit 1
}

# Validate pivot schema version
check_pivot_release=0
# Validate non-pivot schema version
check_non_pivot_release=0
# By default, do not check cta-catalogue-schema submodule tags
check_catalogue_submodule_tags=0
while getopts "pnt" o; do
  case "${o}" in
    p)
      check_pivot_release=1
      ;;
    n)
      check_non_pivot_release=1
      ;;
    t)
      check_catalogue_submodule_tags=1
      ;;
    *)
      usage
      ;;
  esac
done
shift $((OPTIND - 1))

if [[ $# -ne 1 ]]; then
  usage
fi

cta_repo_dir=$1
if ! test -d ${cta_repo_dir}; then
  echo "Error: directory '${cta_repo_dir}' does not exist"
  usage
fi
cta_repo_dir=$(readlink -f $cta_repo_dir)

if [[ "$check_pivot_release" -eq "1" ]] && [[ "$check_non_pivot_release" -eq "1" ]]; then
  echo "Error: Can't set both '-p' and '-c'"
  usage
fi

# Extract variable directly set on a cmake file
extract_cmake_set_val() {
  file_path=$1
  variable=$2
  echo $(grep '^[[:blank:]]*[^[:blank:]#]' $file_path | grep set | grep $variable | sed "s/.*${variable}[[:blank:]][[:blank:]]*\(.*\)[[:blank:]]*)/\1/" | tr ";" " ")
}

# Get CTA catalogue version, as defined in the submodule 'cta-catalogue-schema'
CTA_SUB_REPO__CATALOGUE_MAJOR_VERSION=$(extract_cmake_set_val ${cta_repo_dir}/catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake CTA_CATALOGUE_SCHEMA_VERSION_MAJOR)
CTA_SUB_REPO__CATALOGUE_MINOR_VERSION=$(extract_cmake_set_val ${cta_repo_dir}/catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake CTA_CATALOGUE_SCHEMA_VERSION_MINOR)
# Get all tags from current commit of submodule 'cta-catalogue-schema', separated by whitespaces, and extract MAJOR version
CTA_SUB_REPO__TAGS=$(
  cd ${cta_repo_dir}/catalogue/cta-catalogue-schema
  git fetch --tags --force
  git tag
)

# Get the CTA catalogue version info from the project.json
CTA_PROJECT_CATALOGUE_VERSION=$(jq .catalogueVersion ${cta_repo_dir}/project.json)
CTA_PROJECT_CATALOGUE_MAJOR_VERSION=${CTA_PROJECT_CATALOGUE_VERSION%%.*}
CTA_PROJECT_SUPPORTED_CATALOGUE_VERSIONS=$(jq -r .supportedCatalogueVersions[] ${cta_repo_dir}/project.json)
CTA_PROJECT_PREV_CATALOGUE_VERSIONS=$(echo $CTA_PROJECT_SUPPORTED_CATALOGUE_VERSIONS | grep -v -x $CTA_PROJECT_CATALOGUE_VERSION || true)
CTA_PROJECT_NUM_SUPPORTED_VERSIONS=$(jq '.supportedCatalogueVersions | length' ${cta_repo_dir}/project.json)

#### Start checks ####

echo "Checking..."
# Always check that the CTA catalogue schema version is the same in both the main CTA project and the 'cta-catalogue-schema' submodule
echo -n "- CTA catalogue schema version is the same in the project.json and 'cta-catalogue-schema' submodule: "
if [[ "$CTA_PROJECT_CATALOGUE_VERSION" != "${CTA_SUB_REPO__CATALOGUE_MAJOR_VERSION}.${CTA_SUB_REPO__CATALOGUE_MINOR_VERSION}" ]]; then
  error="${error}CTA catalogue schema version is not the same in the project.json and the 'cta-catalogue-schema' submodule.
         ${CTA_PROJECT_CATALOGUE_VERSION} vs ${CTA_SUB_REPO__CATALOGUE_MAJOR_VERSION}.${CTA_SUB_REPO__CATALOGUE_MINOR_VERSION}\n"
  echo "FAIL"
else
  echo "OK"
fi
echo -n "- CTA catalogue schema version is part of supported catalogue versions: "
if ! echo "${CTA_PROJECT_SUPPORTED_CATALOGUE_VERSIONS}" | grep -q -w "$CTA_PROJECT_CATALOGUE_MAJOR_VERSION"; then
  error="${error}The CTA catalogue schema version must be part of the supportedCatalogueVersions.\n"
  echo "FAIL"
else
  echo "OK"
fi

# [Optional] Check that the 'cta-catalogue-schema' submodule version is tagged
if [[ "$check_catalogue_submodule_tags" -eq "1" ]]; then
  echo -n "- CTA catalogue schema version is tagged in the 'cta-catalogue-schema' submodule commit: "
  if test 0 == $(echo $CTA_SUB_REPO__TAGS | grep $CTA_PROJECT_CATALOGUE_VERSION | wc -l); then
    error="${error}The 'cta-catalogue-schema' submodule commit does not contain a tag for CTA catalogue schema version ${CTA_PROJECT_CATALOGUE_VERSION}.\n"
    echo "FAIL"
  else
    echo "OK"
  fi
fi

# [Optional] Non-pivot release check
# - Can only support 1 catalogue schema version
if [[ "$check_non_pivot_release" -eq "1" ]]; then
  echo -n "- CTA release is not pivot (single catalogue version supported): "
  if [[ "$CTA_PROJECT_NUM_SUPPORTED_VERSIONS" -ne "1" ]]; then
    error="${error}Non-pivot CTA release should only support 1 catalogue schema version. It currently supports multiple versions {${CTA_PROJECT_SUPPORTED_CATALOGUE_VERSIONS}}.\n"
    echo "FAIL"
  else
    echo "OK"
  fi
fi

# [Optional] Pivot release check
# - Supports 2+ catalogue schema versions
# - 'supportedCatalogueVersions' is a list of previous versions that can be migrated to 'CTA_CATALOGUE_REF_SCHEMA_VERSION_CURR'
# - 'CTA_PROJECT_PREV_CATALOGUE_VERSIONS' values must be smaller than 'CTA_PROJECT_CATALOGUE_MAJOR_VERSION'
if [[ "$check_pivot_release" -eq "1" ]]; then
  echo -n "- CTA release is pivot (multiple catalogue versions supported): "
  if [[ "$CTA_PROJECT_NUM_SUPPORTED_VERSIONS" -le "1" ]]; then
    error="${error}Pivot CTA release should support more than 1 catalogue schema versions in the project.json.\n"
    echo "FAIL"
  else
    echo "OK"
  fi
  echo "- All supported releases are less than or equal to the current version: "
  for PREV_VERSION in $(echo $CTA_PROJECT_PREV_CATALOGUE_VERSIONS); do
    if [[ "$(printf '%s\n' "$PREV_VERSION" "$CTA_PROJECT_CATALOGUE_VERSION" | sort -V | head -n1)" != "$PREV_VERSION" ]]; then
      error="${error}Previous CTA catalogue schema version ${PREV_VERSION} is equal or greater than current version ${CTA_PROJECT_CATALOGUE_VERSION}.\n"
      echo "  - FAIL(${PREV_VERSION}->${CTA_PROJECT_CATALOGUE_VERSION}) "
    else
      echo "  - OK(${PREV_VERSION}->${CTA_PROJECT_CATALOGUE_VERSION}) "
    fi
  done
fi

# Validate that there are migration scripts between all versions
CURR_VERSION=${CTA_PROJECT_CATALOGUE_MAJOR_VERSION}
echo "- If schema migration scripts exist: "
for PREV_VERSION in $(echo $CTA_PROJECT_PREV_CATALOGUE_VERSIONS); do
  migration_script_name_1="${PREV_VERSION}to${CURR_VERSION}.sql"
  migration_script_name_2="${PREV_VERSION}.0to${CURR_VERSION}.0.sql"
  migration_scripts_dir="${cta_repo_dir}/catalogue/cta-catalogue-schema/migrations/liquibase"
  if [[ "$PREV_VERSION" -eq "$CURR_VERSION" ]]; then
    # Skip if versions are the same
    echo "  - No script needed for non-pivot release: OK"
    continue
  fi
  if [[ ! -f "${migration_scripts_dir}/oracle/${migration_script_name_1}" ]] && [[ ! -f "${migration_scripts_dir}/oracle/${migration_script_name_2}" ]]; then
    error="${error}Missing oracle migration script from CTA catalogue schema version ${PREV_VERSION} to ${CURR_VERSION}.\n"
    echo "  - Oracle(${PREV_VERSION}->${CURR_VERSION}): FAIL"
  else
    echo "  - Oracle(${PREV_VERSION}->${CURR_VERSION}): OK"
  fi
  if [[ ! -f "${migration_scripts_dir}/postgres/${migration_script_name_1}" ]] && [[ ! -f "${migration_scripts_dir}/postgres/${migration_script_name_2}" ]]; then
    error="${error}Missing postgres migration script from CTA catalogue schema version ${PREV_VERSION} to ${CURR_VERSION}.\n"
    echo "  - Postgres(${PREV_VERSION}->${CURR_VERSION}): FAIL"
  else
    echo "  - Postgres(${PREV_VERSION}->${CURR_VERSION}): OK"
  fi
done

# Fail if there were error...
if [[ -n "${error}" ]]; then
  echo -e "Errors:\n${error}"
  exit 1
fi

echo "Success: All checks succeeded!"
exit 0
