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

die() { echo "$@" 1>&2 ; exit 1; }

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

# create tmp dir for all temporary files
tempdir=$(mktemp -d)

# Does kubernetes version supports `get pod --show-all=true`?
# use it for older versions and this is not needed for new versions of kubernetes
KUBECTL_DEPRECATED_SHOWALL=$(kubectl get pod --show-all=true >/dev/null 2>&1 && echo "--show-all=true")
test -z ${KUBECTL_DEPRECATED_SHOWALL} || echo "WARNING: you are running a old version of kubernetes and should think about updating it."

# Get Catalogue Schema version
MAJOR=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MAJOR ../../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
MINOR=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MINOR ../../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
NEW_SCHEMA_VERSION="$MAJOR.$MINOR"
MIGRATION_FILE=$(find ../../../catalogue/ -name "*to${NEW_SCHEMA_VERSION}.sql")
PREVIOUS_SCHEMA_VERSION=$(echo $MIGRATION_FILE | grep -o -E '[0-9]+\.[0-9]' | head -1)

# Get yum repositories from current commit (will go to standard /shared/yum.repos.d directory in cta-catalogue-updater container)
YUM_REPOS="$(realpath "$(find "$(dirname "$0")"/../../ -name "yum.repos.d")")"
kubectl -n ${NAMESPACE} create configmap yum.repos.d-config --from-file=${YUM_REPOS}
# Add our intermediate script (do we really need it?)
SCRIPT_FILE="$(realpath "$(find "$(dirname "$0")"/../../ -name "dbupdatetest.sh")")"
kubectl -n ${NAMESPACE} create configmap dbupdatetestscript-config --from-file=${SCRIPT_FILE}
# Add configmap taken from ctafrontend pod (will go to standard /shared/etc_cta/cta-catalogue.conf file in cta-catalogue-updater container)
kubectl cp -n ${NAMESPACE} ctafrontend:/etc/cta/cta-catalogue.conf ${tempdir}/cta-catalogue.conf
kubectl -n ${NAMESPACE} create configmap cta-catalogue-config --from-file=${tempdir}/cta-catalogue.conf

# Modify fields of pod yaml with the current data
cp ../pod-dbupdatetest.yaml ${tempdir}
sed -i "s/CATALOGUE_SOURCE_VERSION_VALUE/${PREVIOUS_SCHEMA_VERSION}/g" ${tempdir}/pod-dbupdatetest.yaml
sed -i "s/CATALOGUE_DESTINATION_VERSION_VALUE/${NEW_SCHEMA_VERSION}/g" ${tempdir}/pod-dbupdatetest.yaml

COMMITID=$(git submodule status | grep cta-catalogue-schema | awk '{print $1}')
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

echo -n "Waiting for dbupdatetest pod to be created"
for ((i=0; i<240; i++)); do
  echo -n "."
  # exit loop when all pods are in Running state
  kubectl --namespace=${instance} get pod dbupdatetest ${KUBECTL_DEPRECATED_SHOWALL} -o json | jq -r .status.phase | grep -q -v Running || break
  sleep 1
done

echo -n "Waiting for dbupdatetest to be ready"
for ((i=0; i<400; i++)); do
  echo -n "."
  kubectl -n ${NAMESPACE} logs dbupdatetest | egrep -q "dbupdatetest pod is ready" && break
  sleep 1
done
echo "\n"

kubectl -n ${NAMESPACE} exec -it dbupdatetest -- /bin/bash -c "/launch_liquibase.sh \"tag --tag=test_update\""
kubectl -n ${NAMESPACE} exec -it dbupdatetest -- /bin/bash -c "/launch_liquibase.sh update"

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

kubectl -n ${NAMESPACE} exec -it dbupdatetest -- /bin/bash -c "/launch_liquibase.sh \"rollback --tag=test_update\""

# Check if the current schema version is the same as the previous one
CURRENT_SCHEMA_VERSION=$(kubectl -n ${NAMESPACE} exec ctafrontend -- cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf \
  | grep -o -E '[0-9]+\.[0-9]')

if [ ${CURRENT_SCHEMA_VERSION} ==  ${PREVIOUS_SCHEMA_VERSION} ]; then
  echo "The current Catalogue Schema Version is: ${CURRENT_SCHEMA_VERSION}"
else
  echo "Error. Unexpected Catalogue Schema Version: ${CURRENT_SCHEMA_VERSION}, it should be: ${PREVIOUS_SCHEMA_VERSION}"
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
