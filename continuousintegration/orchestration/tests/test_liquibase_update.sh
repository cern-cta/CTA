#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022-2024 CERN
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

# Note that this tests relies on the cta-catalogue schema being set to the previous version using the -u flag in create_instance

set -e

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

die() { echo "$@" 1>&2 ; exit 1; }

# Define a function to check the current schema version
check_schema_version() {
  CTA_FRONTEND_POD="cta-frontend-0"
  DESIRED_SCHEMA_VERSION=$1
  # Get the current schema version
  CURRENT_SCHEMA_VERSION=$(kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf \
    | grep -o -E '[0-9]+\.[0-9]')

  # Check if the current schema version is the same as the previous one
  if [ ${CURRENT_SCHEMA_VERSION} ==  ${DESIRED_SCHEMA_VERSION} ]; then
    echo "The current Catalogue Schema Version is: ${CURRENT_SCHEMA_VERSION}"
  else
    echo "Error. Unexpected Catalogue Schema Version: ${CURRENT_SCHEMA_VERSION}, it should be: ${DESIRED_SCHEMA_VERSION}"
    exit 1
  fi
}

while getopts "n:" o; do
  case "${o}" in
    n)
      NAMESPACE=${OPTARG}
      ;;
    *)
      usage
      ;;
  esac
done
shift $((OPTIND-1))

if [ -z "${NAMESPACE}" ]; then
  usage
fi


# Note that this assumes the setup was spawned with the previous catalogue version

# Get Catalogue Schema version
project_json_file="../../../project.json"
catalogue_schema_version=$(jq .catalogueVersion ${project_json_file})
prev_catalogue_schema_version=$(jq .supportedCatalogueVersions[] ${project_json_file} | grep -v $catalogue_schema_version | head -1)
defaultPlatform=$(jq -r .dev.defaultPlatform ${project_json_file})

echo "Checking if the current schema version is the same as the previous one"
check_schema_version ${prev_catalogue_schema_version}

# This is pretty disgusting but for now this will do
# If the configmap generation would be done through Helm the file in question needs to be within the chart
yum_repos_file="$(realpath "$(dirname "$0")/../../docker/${defaultPlatform}/etc/yum.repos.d")"
kubectl -n ${NAMESPACE} create configmap yum.repos.d-config --from-file=${yum_repos_file}

# Set up the catalogue updater pod
helm install catalogue-updater ../helm/catalogue-updater --namespace ${NAMESPACE} \
                                                         --set catalogueSourceVersion=$prev_catalogue_schema_version \
                                                         --set catalogueDestinationVersion=$catalogue_schema_version \
                                                         --wait --timeout 2m

kubectl -n ${NAMESPACE} exec -it liquibase-update -- /bin/bash -c "/launch_liquibase.sh \"tag --tag=test_update\""
kubectl -n ${NAMESPACE} exec -it liquibase-update -- /bin/bash -c "/launch_liquibase.sh update"

# Check if the current schema version is the same as the new one. If it is, the update was successful
echo "Checking if liquibase update was successful"
check_schema_version ${catalogue_schema_version}

kubectl -n ${NAMESPACE} exec -it liquibase-update -- /bin/bash -c "/launch_liquibase.sh \"rollback --tag=test_update\""

# Check if the current schema version is the same as the previous one. Rollback should be successful.
echo "Checking if liquibase rollback was successful"
check_schema_version ${prev_catalogue_schema_version}

echo "Liquibase update and rollback were successful"

exit 0
