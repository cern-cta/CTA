#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

set -e

# CTA registry secret name
ctareg_secret='ctaregsecret'

# defaults scheduler datastore to objectstore (using a file)
config_schedstore="/opt/kubernetes/CTA/objectstore/objectstore-file.yaml"
# defaults DB to sqlite
config_database="/opt/kubernetes/CTA/database/oracle-creds.yaml"
# default library model
model="mhvtl"
# defaults MGM namespace to quarkdb with http
config_eos="./eos5-config-quarkdb-https.yaml"

# EOS short instance name
EOSINSTANCE=ctaeos

# By default to not use systemd to manage services inside the containers
usesystemd=0

# By default keep Database and keep Scheduler datastore data
# default should not make user loose data if he forgot the option
keepdatabase=1
keepobjectstore=1

# By default run the standard test no oracle dbunittests
runoracleunittests=0

# By default doesn't prepare the images with the previous schema version
updatedatabasetest=0

# Create an instance for
systest_only=0

# Number of tape servers to spawn
tpsrv_count=2
# for the pod images
REGISTRY_HOST="gitlab-registry.cern.ch/cta"

die() { echo "$@" 1>&2 ; exit 1; }

usage() {
  echo "Script for managing Kubernetes resources."
  echo ""
  echo "Usage: $0 -n <namespace> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                     Shows help output."
  echo "  -n <namespace>:                 Specify the Kubernetes namespace."
  echo "  -o <schedstore_configmap>:      Path to the scheduler configmap file."
  echo "  -d <database_configmap>:        Path to the database configmap file."
  echo "  -e <eos_configmap>:             Path to the EOS configmap file."
  echo "  -a <additional_k8_resources>:   Path to additional Kubernetes resources."
  echo "  -p <gitlab pipeline ID>:        GitLab pipeline ID."
  echo "  -i <docker image tag>:          Docker image tag for the deployment."
  echo "  -r <docker registry>:           Provide the Docker registry. Defaults to \"gitlab-registry.cern.ch/cta\"."
  echo "  -c <tpsrv count>:               Set the number of tape servers to spawn. Defaults to 2."
  echo "  -S:                             Use systemd to manage services inside containers."
  echo "  -D:                             Wipe database content during initialization phase (content is kept by default)."
  echo "  -O:                             Wipe scheduler datastore (objectstore or postgres) content during initialization phase (content is kept by default)."
  echo "  -U:                             Run database unit tests only."
  echo "  -u:                             Prepare the pods to run the Liquibase test."
  echo "  -Q:                             Create the cluster using the last ctageneric image from main."
  exit 1
}

while getopts "n:o:d:e:p:b:i:r:c:SDOUumQ" o; do
    case "${o}" in
        o)
            config_schedstore=${OPTARG} ;;
        d)
            config_database=${OPTARG} ;;
        e)
            config_eos=${OPTARG} ;;
        m)
            model=${OPTARG} ;;
        n)
            instance=${OPTARG} ;;
        p)
            pipelineid=${OPTARG} ;;
        i)
            dockerimage=${OPTARG} ;;
        r)
            REGISTRY_HOST=${OPTARG} ;;
        c)
            tpsrv_count=${OPTARG} ;;
        S)
            usesystemd=1 ;;
        O)
            keepobjectstore=0 ;;
        D)
            keepdatabase=0 ;;
        U)
            runoracleunittests=1 ;;
        u)
            updatedatabasetest=1 ;;
        Q)
            systest_only=1 ;;
        *)
            usage ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${instance}" ]; then
    usage
fi

if [ -n "${pipelineid}" ] && [ -n "${dockerimage}" ]; then
    usage
fi

test -f ${config_schedstore} || die "Scheduler database credentials file ${config_schedstore} does not exist\n"
test -f ${config_database} || die "Database configmap file ${config_database} does not exist\n"
test -f ${config_eos} || die "$EOS configmap file ${config_eos} does not exist\n"
if [ "-${model}-" != "-ibm-" ] && [ "-${model}-" != "-mhvtl-" ] ; then die "Library model ${model} does not exist\n"; fi

if ! command -v helm >/dev/null 2>&1; then
    echo "ERROR: Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
    exit 1
fi

# Get Catalogue Schema version
MAJOR=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MAJOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
MINOR=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MINOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
SCHEMA_VERSION="$MAJOR.$MINOR"

# It sets as schema version the previous to the current one to create a database with that schema version
if [[ "$updatedatabasetest" == "1" ]] ; then
  MIGRATION_FILE=$(find ../../catalogue/cta-catalogue-schema -name "*to${SCHEMA_VERSION}.sql")
  PREVIOUS_SCHEMA_VERSION=$(echo $MIGRATION_FILE | grep -o -E '[0-9]+\.[0-9]' | head -1)
  SCHEMA_VERSION=$PREVIOUS_SCHEMA_VERSION
  echo "Deploying with previous catalogue schema version: ${SCHEMA_VERSION}"
else
  echo "Deploying with current catalogue schema version: ${SCHEMA_VERSION}"
fi

# We are going to run with repository based images (they have rpms embedded)
../ci_helpers/get_registry_credentials.sh --check > /dev/null || { echo "Error: Credential check failed"; exit 1; }
if [[ ${systest_only} -eq 1 ]]; then
  COMMITID=$(curl --url "https://gitlab.cern.ch/api/v4/projects/139306/repository/commits" | jq -cr '.[0] | .short_id' | sed -e 's/\(........\).*/\1/')
else
  COMMITID=$(git log -n1 | grep ^commit | cut -d\  -f2 | sed -e 's/\(........\).*/\1/')
fi
if [[ "${systest_only}" -eq 1 ]]; then
  echo "Creating instance from image build for lastest commit on main ${COMMITID}"
  imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${COMMITID} | tail -n1)
elif [ -n "${pipelineid}" ]; then
  echo "Creating instance for image built on commit ${COMMITID} with gitlab pipeline ID ${pipelineid}"
  imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${COMMITID} | grep ^${pipelineid}git | sort -n | tail -n1)
  # just a shortcut to avoid time lost checking against the docker registry...
  #imagetag=${pipelineid}git${COMMITID}
elif [ -n "${dockerimage}" ]; then
  echo "Creating instance for image ${dockerimage}"
  imagetag=${dockerimage}
else
  echo "Creating instance for latest image built for ${COMMITID} (highest PIPELINEID)"
  imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${COMMITID} | sort -n | tail -n1)
fi
if [ "${imagetag}" == "" ]; then
  echo "commit:${COMMITID} has no docker image available in gitlab registry, please check pipeline status and registry images available."
  exit 1
fi

if [ $usesystemd == 1 ] ; then
    echo "Using systemd to start services on some pods"
    # TODO: this shouldn't be happening by a find-replace, but rather by updating the values.yml
    # E.g. either setting an option or using a different values.yml
    # Additionally, this doesn't even work in the first place as these files don't exist anymore
    # for podname in ctafrontend tpsrv ctaeos; do
    #     sed -i "/^\ *command:/d" ${poddir}/pod-${podname}*.yaml
    # done
    die "systemd support has not yet been implement in the Helm setup"
fi

if [ $keepdatabase == 1 ] ; then
    # TODO: this does not seem to be implemented yet
    echo "DB content will be kept"
else
    echo "DB content will be wiped"
fi

if [ $keepobjectstore == 1 ] ; then
    # TODO: this does not seem to be implemented yet
    echo "scheduler data store content will be kept"
else
    echo "schedule data store content will be wiped"
fi

echo "Creating ${instance} namespace"
kubectl create namespace ${instance}

# The registry secret(s) must be copied in the instance namespace to be usable
kubectl get secret ${ctareg_secret} &> /dev/null
if [ $? -eq 0 ]; then
  echo "Copying ${ctareg_secret} secret in ${instance} namespace"
  kubectl get secret ctaregsecret -o yaml | grep -v '^ *namespace:' | kubectl --namespace ${instance} create -f -
fi

# TODO: how can this be done in the init chart?
# Setting up library
echo "Requesting an unused ${model} library"
kubectl create -f ./pvc_library_${model}.yaml --namespace=${instance}
for ((i=0; i<120; i++)); do
  echo -n "."
  kubectl get persistentvolumeclaim claimlibrary --namespace=${instance} | grep -q Bound && break
  sleep 1
done
kubectl get persistentvolumeclaim claimlibrary --namespace=${instance} | grep -q Bound || die "TIMED OUT"
echo "OK"
LIBRARY_DEVICE=$(kubectl get persistentvolumeclaim claimlibrary --namespace=${instance} -o json | jq -r '.spec.volumeName')
echo "Get library device: ${LIBRARY_DEVICE}"
kubectl --namespace=${instance} create -f /opt/kubernetes/CTA/library/config/library-config-${LIBRARY_DEVICE}.yaml
echo "Got library: ${LIBRARY_DEVICE}"

echo  "Installing init chart..."
helm install init helm/init -n ${instance} \
                            --set global.image.registry=${REGISTRY_HOST} \
                            --set global.image.tag=${imagetag} \
                            --set catalogue.schemaVersion="${SCHEMA_VERSION}" \
                            -f ${config_schedstore} \
                            -f ${config_database} \
                            --wait --timeout 5m

# TODO: move this outside of this script
if [ $runoracleunittests == 1 ] ; then
  # everyone needs poddir temporary directory to generate pod yamls
  poddir=$(mktemp -d)
  echo 'copying files to tmpdir'
  cp ./pod-oracleunittests.yaml ${poddir}
  # For now we do a search replace of the image tag for this pod.
  # Note that this does not replace the registry, so this pod cannot be used with a local image (yet).
  # Eventually this should also be integrated into helm and be overriden using the --set flag.
  # Then the tmp dir is also no longer necessary
  # e.g. helm install oracleunittest --wait --timeout 5m --set global.image=${IMAGE} \
  sed -i $poddir/pod-oracleunittests.yaml -e "s/ctageneric\:.*/ctageneric:${imagetag}/g"
  echo "Running database unit-tests"
  kubectl create -f ${poddir}/pod-oracleunittests.yaml --namespace=${instance}

  echo -n "Waiting for oracleunittests"
  for ((i=0; i<400; i++)); do
    echo -n "."
    kubectl --namespace=${instance} get pod oracleunittests -o json | jq -r .status.phase | grep -E -q 'Succeeded|Failed' && break
    sleep 1
  done
  echo ""

  kubectl --namespace=${instance} logs oracleunittests

  # database unit-tests went wrong => exit now with error
  if $(kubectl --namespace=${instance} get pod oracleunittests -o json | jq -r .status.phase | grep -q Failed); then
    echo "oracleunittests pod in Error status here are its last log lines:"
    kubectl --namespace=${instance} logs oracleunittests --tail 10
    die "ERROR: oracleunittests pod in Error state. Initialization failed."
  fi
  # database unit-tests were successful => exit now with success
  exit 0
fi

# This simply counts the number of drives in the mhvtl config
# If their slots do not start from 0 (which they should), this will not produce the expected results
num_drives_available=$(grep Drive -c /etc/mhvtl/device.conf)

echo ""
echo "Processing dependencies of cta chart..."
helm dependency update helm/cta
echo "Installing cta chart..."
helm install cta helm/cta -n ${instance} \
                          --set global.image.registry=${REGISTRY_HOST} \
                          --set global.image.tag=${imagetag} \
                          --set tpsrv.tpsrv.replicaCount=${tpsrv_count} \
                          --set tpsrv.tpsrv.numDrivesAvailable=${num_drives_available} \
                          --wait --timeout 5m

kubectl get pods -n ${instance}

# Set up kerberos: TODO can this be done in helm cta chart setup script
echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${instance} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
echo -n "Using kinit for ctacli and client"
kubectl --namespace=${instance} exec ctacli -- kinit -kt /root/ctaadmin1.keytab ctaadmin1@TEST.CTA
kubectl --namespace=${instance} exec client -- kinit -kt /root/user1.keytab user1@TEST.CTA
# space=${instance} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
# May be needed for the client to make sure that SSS is not used by default but krb5...
#echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${instance} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
echo "klist for client:"
kubectl --namespace=${instance} exec client -- klist
echo "klist for ctacli:"
kubectl --namespace=${instance} exec ctacli -- klist


# TODO: why is this not done in the ctaoes sartup script?
# Set the workflow rules for archiving, creating tape file replicas in the EOS namespace, retrieving
# files from tape and deleting files.
echo "Setting workflows in namespace ${instance} pod ctaeos:"
CTA_WF_DIR=/eos/${EOSINSTANCE}/proc/cta/workflow
for WORKFLOW in sync::create.default sync::closew.default sync::archived.default sync::archive_failed.default sync::prepare.default sync::abort_prepare.default sync::evict_prepare.default sync::closew.retrieve_written sync::retrieve_failed.default sync::delete.default
do
  echo "eos attr set sys.workflow.${WORKFLOW}=\"proto\" ${CTA_WF_DIR}"
  kubectl --namespace=${instance} exec ctaeos -- bash -c "eos attr set sys.workflow.${WORKFLOW}=\"proto\" ${CTA_WF_DIR}"
done

echo "Instance ${instance} successfully created:"
kubectl --namespace=${instance} get pods
