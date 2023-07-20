#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2023 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

# Validate pivot schema version
check_pivot_release=0
# Validate non-pivot schema version
check_non_pivot_release=0
# By default, do not check cta-catalogue-schema submodule tags
check_catalogue_submodule_tags=0

usage() { cat <<EOF 1>&2
Usage: $0 [-p|-n] [-t] <cta_repo_dir>
Options:
  <cta_repo_dir>    Path to CTA repository
  -p                Validate pivot release
  -n                Validate non-pivot release
  -t                Validate CTA catalogue schema submodule tag.
EOF
exit 1
}

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
shift $((OPTIND-1))

if [ $# -ne 1 ]; then
    usage
fi

cta_repo_dir=$1
if ! test -d ${cta_repo_dir}; then
  echo "Error: directory '${cta_repo_dir}' does not exist"
  usage
  exit 1
fi
cta_repo_dir=$(readlink -f $cta_repo_dir)

if [ "$check_pivot_release" -eq "1" ] && [ "$check_non_pivot_release" -eq "1" ]; then
  echo "Error: Can't set both '-p' and '-c'"
  usage
  exit 1
fi

#echo "Directory:               " $cta_repo_dir
#echo "Check pivot release:     " $([[ $check_pivot_release = 0 ]]            && echo "False" || echo "True")
#echo "Check non-pivot release: " $([[ $check_non_pivot_release = 0 ]]        && echo "False" || echo "True")
#echo "Check tag:               " $([[ $check_catalogue_submodule_tags = 0 ]] && echo "False" || echo "True")

# Extract variable directly set on a cmake file
extract_cmake_set_val() {
  file_path=$1
  variable=$2
  echo $(grep '^[[:blank:]]*[^[:blank:]#]' $file_path | grep set | grep $variable | sed "s/.*${variable}[[:blank:]][[:blank:]]*\(.*\)[[:blank:]]*)/\1/" | tr ";" " ")
}

# Get current CTA catalogue schema version, from main CTA repo
CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR=$(extract_cmake_set_val ${cta_repo_dir}/catalogue/CTARefCatalogueSchemaVersion.cmake CTA_CATALOGUE_REF_SCHEMA_VERSION_CURR)
# Get previous CTA catalogue schema versions (can be a whitespace-separated list), from main CTA repo
CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_PREV_L=$(extract_cmake_set_val ${cta_repo_dir}/catalogue/CTARefCatalogueSchemaVersion.cmake CTA_CATALOGUE_REF_SCHEMA_VERSION_PREV)

# Get CTA catalogue version, as defined in the submodule 'cta-catalogue-schema'
CTA_SUB_REPO__CATALOGUE_MAJOR_VERSION=$(extract_cmake_set_val ${cta_repo_dir}/catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake CTA_CATALOGUE_SCHEMA_VERSION_MAJOR)
# Get all tags from current commit of submodule 'cta-catalogue-schema', separated by whitespaces, and extract MAJOR version
CTA_SUB_REPO__TAGS_L=$(git -C ${cta_repo_dir}/catalogue/cta-catalogue-schema tag | sed 's/v//g;s/\(\.[0-9]*\)//g' | xargs)

#### Start checks ####

echo "Checking..."
# Always check that the CTA catalogue schema version is the same in both the main CTA project and the 'cta-catalogue-schema' submodule
echo -n "- CTA catalogue schema version is the same in 'CTA' repo and 'cta-catalogue-schema' submodule: "
if [ "$CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR" -ne "$CTA_SUB_REPO__CATALOGUE_MAJOR_VERSION" ]; then
  error="${error}CTA catalogue schema version is not the same in the main CTA project (${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR}) and the 'cta-catalogue-schema' submodule ${CTA_SUB_REPO__CATALOGUE_MAJOR_VERSION}.\n"
  echo "FAIL"
else
  echo "OK"
fi

# [Optional] Check that the 'cta-catalogue-schema' submodule version is tagged
if [ "$check_catalogue_submodule_tags" -eq "1" ]; then
  echo -n "- CTA catalogue schema version is tagged in the 'cta-catalogue-schema' submodule commit: "
  if test 0 == $(echo $CTA_SUB_REPO__TAGS_L | xargs -n1 | grep -w $CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR | wc -l); then
    error="${error}The 'cta-catalogue-schema' submodule commit does not contain a tag for CTA catalogue schema version ${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR}.\n"
    echo "FAIL"
  else
    echo "OK"
  fi
fi

# [Optional] Non-pivot release check
# - Can only support 1 catalogue schema version
# - 'CTA_CATALOGUE_REF_SCHEMA_VERSION_CURR' must be the same as 'CTA_CATALOGUE_REF_SCHEMA_VERSION_PREV'
if [ "$check_non_pivot_release" -eq "1" ]; then
  echo -n "- CTA release is not pivot (single catalogue version supported): "
  if [ "${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR}" -ne "${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_PREV_L}" ]; then
    supported_versions=$(echo "${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR} ${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_PREV_L}" | xargs -n1 | sort -u | xargs | sed 's/ /, /g')
    error="${error}Non-pivot CTA release should only support 1 catalogue schema version. It currently supports multiple versions {${supported_versions}}.\n"
    echo
  fi
fi

# [Optional] Pivot release check
# - Supports 2+ catalogue schema versions
# - 'CTA_CATALOGUE_REF_SCHEMA_VERSION_PREV' is a list of previous versions that can be migrated to 'CTA_CATALOGUE_REF_SCHEMA_VERSION_CURR'
# - 'CTA_CATALOGUE_REF_SCHEMA_VERSION_PREV' values must be smaller than 'CTA_CATALOGUE_REF_SCHEMA_VERSION_CURR'
if [ "$check_pivot_release" -eq "1" ]; then
  echo -n "- CTA release is pivot (multiple catalogue versions supported): "
  for PREV_VERSION in $(echo $CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_PREV_L); do
    if [ "${PREV_VERSION}" -ge "$CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR" ]; then
      error="${error}Previous CTA catalogue schema version ${PREV_VERSION} is equal or greater than current version ${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR}.\n"
      echo -n "FAIL(${PREV_VERSION}->${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR}) "
    else
      echo -n "OK(${PREV_VERSION}->${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR}) "
    fi
  done
  echo ""
fi

# Validate that there are migration scripts between all versions
CURR_VERSION=${CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_CURR}
echo "- Migration scripts exist: "
for PREV_VERSION in $(echo $CTA_MAIN_REPO__CATALOGUE_MAJOR_VERSION_PREV_L); do
  migration_script_name_1="${PREV_VERSION}to${CURR_VERSION}.sql"
  migration_script_name_2="${PREV_VERSION}.0to${CURR_VERSION}.0.sql"
  migration_scripts_dir="${cta_repo_dir}/catalogue/cta-catalogue-schema/migrations/liquibase"
  if [ "$PREV_VERSION" -eq "$CURR_VERSION" ]; then
    # Skip if versions are the same
    echo "  - No script needed for non-pivot release: OK"
    continue
  fi
  if [ ! -f "${migration_scripts_dir}/oracle/${migration_script_name_1}" ] && [ ! -f "${migration_scripts_dir}/oracle/${migration_script_name_2}" ]; then
    error="${error}Missing oracle migration script from CTA catalogue schema version ${PREV_VERSION} to ${CURR_VERSION}.\n"
    echo "  - Oracle(${PREV_VERSION}->${CURR_VERSION}): FAIL"
  else
    echo "  - Oracle(${PREV_VERSION}->${CURR_VERSION}): OK"
  fi
  if [ ! -f "${migration_scripts_dir}/postgres/${migration_script_name_1}" ] && [ ! -f "${migration_scripts_dir}/postgres/${migration_script_name_2}" ]; then
    error="${error}Missing postgres migration script from CTA catalogue schema version ${PREV_VERSION} to ${CURR_VERSION}.\n"
    echo "  - Postgres(${PREV_VERSION}->${CURR_VERSION}): FAIL"
  else
    echo "  - Postgres(${PREV_VERSION}->${CURR_VERSION}): OK"
  fi
done

# Fail if there were error...
if [ ! -z "${error}" ]; then
    echo -e "Error:\n${error}"
    exit 1
fi

echo "Success: All checks succeeded!"
exit 0