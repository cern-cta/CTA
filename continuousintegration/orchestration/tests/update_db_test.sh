#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
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


usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

CTA_VERSION=""

while getopts "n:v:" o; do
  case "${o}" in
    n)
      NAMESPACE=${OPTARG}
      ;;
    v)
      CTA_VERSION=${OPTARG}
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

if [ ! -z "${error}" ]; then
  echo -e "ERROR:\n${error}"
  exit 1
fi

# Get Catalogue Schema version
MAJOR=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MAJOR ../../../cmake/CTAVersions.cmake | sed 's/[^0-9]*//g')
MINOR=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MINOR ../../../cmake/CTAVersions.cmake | sed 's/[^0-9]*//g')
NEW_SCHEMA_VERSION="$MAJOR.$MINOR"
MIGRATION_FILE=$(find ../../../catalogue/ -name "*to${NEW_SCHEMA_VERSION}.sql")
PREVIOUS_SCHEMA_VERSION=$(echo $MIGRATION_FILE | grep -o -E '[0-9]+\.[0-9]' | head -1)

YUM_REPOS="$(pwd)/$(find ../../ -name "yum.repos.d")"
SCRIPTS_DIR="$(pwd)/$(find ../../ -name "dbupdatetest.sh" | xargs dirname)"

# Modify fields of pod yaml with the current data
tempdir=$(mktemp -d)
cp ../pod-dbupdatetest.yaml ${tempdir}
sed -i "s#REPOS_PATH#${YUM_REPOS}#g" ${tempdir}/pod-dbupdatetest.yaml
sed -i "s#SCRIPTS_PATH#${SCRIPTS_DIR}#g" ${tempdir}/pod-dbupdatetest.yaml
sed -i "s/CATALOGUE_SOURCE_VERSION_VALUE/${PREVIOUS_SCHEMA_VERSION}/g" ${tempdir}/pod-dbupdatetest.yaml
sed -i "s/CATALOGUE_DESTINATION_VERSION_VALUE/${NEW_SCHEMA_VERSION}/g" ${tempdir}/pod-dbupdatetest.yaml

COMMITID=$(git log -n1 | grep ^commit | cut -d\  -f2 | sed -e 's/\(........\).*/\1/')
sed -i "s/COMMIT_ID_VALUE/${COMMITID}/g" ${tempdir}/pod-dbupdatetest.yaml
sed -i "s/CTA_VERSION_VALUE/${CTA_VERSION}/g" ${tempdir}/pod-dbupdatetest.yaml

# Check if the current schema version is the same as the previous one
CURRENT_SCHEMA_VERSION=$(kubectl -n ${NAMESPACE} exec ctafrontend -- cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf \
  | grep -o -E '[0-9]+\.[0-9]')

if [ ${CURRENT_SCHEMA_VERSION} ==  ${PREVIOUS_SCHEMA_VERSION} ]; then
  echo "The current Catalogue Schema Version is: ${CURRENT_SCHEMA_VERSION}"
else
  echo "Error. Unexpected Catalogue Schema Version: ${CURRENT_SCHEMA_VERSION}, it should be: ${PREVIOUS_SCHEMA_VERSION}"
  exit 1
fi

kubectl create -f ${tempdir}/pod-dbupdatetest.yaml --namespace=${NAMESPACE}

echo -n "Waiting for dbupdatetest"
for ((i=0; i<400; i++)); do
  echo -n "."
  kubectl get pod dbupdatetest -a --namespace=${NAMESPACE} | egrep -q 'Completed|Error' && break
  sleep 1
done
echo "\n"

# Check if the current schema version is the same as the new one
CURRENT_SCHEMA_VERSION=$(kubectl -n ${NAMESPACE} exec ctafrontend -- cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf \
  | grep -o -E '[0-9]+\.[0-9]')
if [ ${CURRENT_SCHEMA_VERSION} ==  ${NEW_SCHEMA_VERSION} ]; then
  echo "The current Catalogue Schema Version is: ${CURRENT_SCHEMA_VERSION}"
  kubectl -n ${NAMESPACE} logs dbupdatetest &> "../../../pod_logs/${NAMESPACE}/liquibase-update.log"
else
  echo "Error. Unexpected Catalogue Schema Version: ${CURRENT_SCHEMA_VERSION}, it should be: ${NEW_SCHEMA_VERSION}"
  kubectl -n ${NAMESPACE} logs dbupdatetest &> "../../../pod_logs/${NAMESPACE}/liquibase-update.log"
  exit 1
fi

# If the previous and new schema has same major version, we can run a simple archive-retrieve test
PREVIOUS_MAJOR=$(echo ${PREVIOUS_SCHEMA_VERSION} | cut -d. -f1)
if [ "${MAJOR}" == "${PREVIOUS_MAJOR}" ] ; then
  echo
  echo "Running a simple archive-retrieve test to check if the update is working"
  echo "Preparing namespace for the tests"
  ./prepare_tests.sh -n ${NAMESPACE}
  if [ $? -ne 0 ]; then
    echo "ERROR: failed to prepare namespace for the tests"
    exit 1
  fi

  echo
  echo "Launching simple_client_ar.sh on client pod"
  echo " Archiving file: xrdcp as user1"
  echo " Retrieving it as poweruser1"
  kubectl -n ${NAMESPACE} cp simple_client_ar.sh client:/root/simple_client_ar.sh
  kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/client_helper.sh
  kubectl -n ${NAMESPACE} exec client -- bash /root/simple_client_ar.sh || exit 1

  kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh ctaeos:/root/grep_xrdlog_mgm_for_error.sh
  kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1
fi

exit 0
