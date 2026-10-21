#!/bin/bash

# SPDX-FileCopyrightText: 2023 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

usage() {
  echo
  echo "Usage: $0 [options]"
  echo
  echo "Validates the CTA repository."
  echo
  echo "options:"
  echo "  -t:                      Validate CTA catalogue schema submodule tag."
  echo
  exit 1
}

# By default, do not check cta-catalogue-schema submodule tags
check_catalogue_submodule_tags=0
while getopts "t" o; do
  case "${o}" in
    t)
      check_catalogue_submodule_tags=1
      ;;
    *)
      usage
      ;;
  esac
done
shift $((OPTIND - 1))

if [[ $# -ne 0 ]]; then
  usage
fi

project_root=$(git rev-parse --show-toplevel)
readonly project_root

# Extract variable directly set on a cmake file
extract_cmake_set_val() {
  file_path=$1
  variable=$2
  echo $(grep '^[[:blank:]]*[^[:blank:]#]' $file_path | grep set | grep $variable | sed "s/.*${variable}[[:blank:]][[:blank:]]*\(.*\)[[:blank:]]*)/\1/" | tr ";" " ")
}

# Get CTA catalogue version, as defined in the submodule 'cta-catalogue-schema'
CTA_SUB_REPO__CATALOGUE_MAJOR_VERSION=$(extract_cmake_set_val ${project_root}/catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake CTA_CATALOGUE_SCHEMA_VERSION_MAJOR)
CTA_SUB_REPO__CATALOGUE_MINOR_VERSION=$(extract_cmake_set_val ${project_root}/catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake CTA_CATALOGUE_SCHEMA_VERSION_MINOR)

# Get the CTA catalogue version info from the project.json
CTA_PROJECT_CATALOGUE_VERSION=$(jq .catalogueVersion ${project_root}/project.json)
CTA_PROJECT_CATALOGUE_MAJOR_VERSION=${CTA_PROJECT_CATALOGUE_VERSION%%.*}
CTA_PROJECT_SUPPORTED_CATALOGUE_VERSIONS=$(jq -r .supportedCatalogueVersions[] ${project_root}/project.json)
CTA_PROJECT_PREV_CATALOGUE_VERSIONS=$(jq -r --argjson current "$CTA_PROJECT_CATALOGUE_MAJOR_VERSION" '.supportedCatalogueVersions[] | select(. != $current)' ${project_root}/project.json)
CTA_PROJECT_NUM_SUPPORTED_VERSIONS=$(jq '.supportedCatalogueVersions | length' ${project_root}/project.json)

#### Start checks ####

error=""
log_task "Checking catalogue schema versions"
# Always check that the CTA catalogue schema version is the same in both the main CTA project and the 'cta-catalogue-schema' submodule
log_task "Checking catalogue schema version matches project.json and the submodule"
if [[ "$CTA_PROJECT_CATALOGUE_VERSION" != "${CTA_SUB_REPO__CATALOGUE_MAJOR_VERSION}.${CTA_SUB_REPO__CATALOGUE_MINOR_VERSION}" ]]; then
  error="${error}CTA catalogue schema version is not the same in the project.json and the 'cta-catalogue-schema' submodule.
         ${CTA_PROJECT_CATALOGUE_VERSION} vs ${CTA_SUB_REPO__CATALOGUE_MAJOR_VERSION}.${CTA_SUB_REPO__CATALOGUE_MINOR_VERSION}\n"
  log_error "FAIL"
else
  log_success "OK"
fi
log_task "Checking catalogue schema version is supported"
if ! echo "${CTA_PROJECT_SUPPORTED_CATALOGUE_VERSIONS}" | grep -q -w "$CTA_PROJECT_CATALOGUE_MAJOR_VERSION"; then
  error="${error}The CTA catalogue schema version must be part of the supportedCatalogueVersions.\n"
  log_error "FAIL"
else
  log_success "OK"
fi

# [Optional] Check that the 'cta-catalogue-schema' submodule version is tagged
if [[ "$check_catalogue_submodule_tags" -eq "1" ]]; then
  # Get all tags from the cta-catalogue-schema submodule.
  CTA_SUB_REPO__TAGS=$(
    cd ${project_root}/catalogue/cta-catalogue-schema
    git fetch --tags --force
    git tag
  )
  log_task "Checking catalogue schema version has a submodule tag"
  if [[ 0 == $(echo $CTA_SUB_REPO__TAGS | grep $CTA_PROJECT_CATALOGUE_VERSION | wc -l) ]]; then
    error="${error}The 'cta-catalogue-schema' submodule commit does not contain a tag for CTA catalogue schema version ${CTA_PROJECT_CATALOGUE_VERSION}.\n"
    log_error "FAIL"
  else
    log_success "OK"
  fi
fi

# Infer the release type from project.json. Pivot releases support multiple
# catalogue schema versions; non-pivot releases support only the current one.
# - 'supportedCatalogueVersions' lists the versions that can be migrated to the current catalogue version.
# - 'CTA_PROJECT_PREV_CATALOGUE_VERSIONS' values must be smaller than 'CTA_PROJECT_CATALOGUE_MAJOR_VERSION'
if [[ "$CTA_PROJECT_NUM_SUPPORTED_VERSIONS" -gt "1" ]]; then
  log_task "CTA release is pivot (multiple catalogue versions supported): ${CTA_PROJECT_SUPPORTED_CATALOGUE_VERSIONS}"
  log_task "Checking previous supported releases are older than the current version"
  for PREV_VERSION in $CTA_PROJECT_PREV_CATALOGUE_VERSIONS; do
    if [[ "$(printf '%s\n' "$PREV_VERSION" "$CTA_PROJECT_CATALOGUE_VERSION" | sort -V | head -n1)" != "$PREV_VERSION" ]]; then
      error="${error}Previous CTA catalogue schema version ${PREV_VERSION} is greater than current version ${CTA_PROJECT_CATALOGUE_VERSION}.\n"
      log_error "FAIL(${PREV_VERSION}->${CTA_PROJECT_CATALOGUE_VERSION}) "
    else
      log_success "OK(${PREV_VERSION}->${CTA_PROJECT_CATALOGUE_VERSION}) "
    fi
  done
else
  log_task "CTA release is not pivot (single catalogue version supported): ${CTA_PROJECT_SUPPORTED_CATALOGUE_VERSIONS}"
fi

# Validate that there are migration scripts between all versions
CURR_VERSION=${CTA_PROJECT_CATALOGUE_MAJOR_VERSION}
log_task "Checking schema migration scripts exist"
for PREV_VERSION in $CTA_PROJECT_PREV_CATALOGUE_VERSIONS; do
  migration_script_name_1="${PREV_VERSION}to${CURR_VERSION}.sql"
  migration_script_name_2="${PREV_VERSION}.0to${CURR_VERSION}.0.sql"
  migration_scripts_dir="${project_root}/catalogue/cta-catalogue-schema/migrations/liquibase"
  if [[ ! -f "${migration_scripts_dir}/oracle/${migration_script_name_1}" ]] && [[ ! -f "${migration_scripts_dir}/oracle/${migration_script_name_2}" ]]; then
    error="${error}Missing oracle migration script from CTA catalogue schema version ${PREV_VERSION} to ${CURR_VERSION}.\n"
    log_error "Oracle(${PREV_VERSION}->${CURR_VERSION}): FAIL"
  else
    log_success "Oracle(${PREV_VERSION}->${CURR_VERSION}): OK"
  fi
  if [[ ! -f "${migration_scripts_dir}/postgres/${migration_script_name_1}" ]] && [[ ! -f "${migration_scripts_dir}/postgres/${migration_script_name_2}" ]]; then
    error="${error}Missing postgres migration script from CTA catalogue schema version ${PREV_VERSION} to ${CURR_VERSION}.\n"
    log_error "Postgres(${PREV_VERSION}->${CURR_VERSION}): FAIL"
  else
    log_success "Postgres(${PREV_VERSION}->${CURR_VERSION}): OK"
  fi
done

# Fail if there were error...
if [[ -n "${error}" ]]; then
  log_error "$(printf 'Errors:\n%b' "${error}")"
  exit 1
fi

log_success "SUCCESS: All catalogue schema checks passed"
exit 0
