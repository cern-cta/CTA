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

# CTA registry secret name
ctareg_secret='ctaregsecret'

# defaults scheduler datastore to objectstore (using a file)
config_schedstore="./objectstore-file.yaml"
# defaults DB to sqlite
config_database="./database-sqlite.yaml"
# default library model
model="mhvtl"
# defaults MGM namespace to quarkdb with http
config_eos="./eos5-config-quarkdb-https.yaml"
# shared configmap for eoscta instance
config_eoscta="./eoscta-config.yaml"
config_script="./sandbox-script.yaml"
keypass_names="./keypass-names-configmap.yaml"
config_kdc="./kdc-krb5.yaml"

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

# By default doesn't run the tests for external tape formats
runexternaltapetests=0

# Create an instance for
systest_only=0

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> [-o <schedstore_configmap>] [-d <database_configmap>] \
      [-e <eos_configmap>] [-a <additional_k8_resources>]\
      [-p <gitlab pipeline ID>] [-i <docker image tag>] \
      [-S] [-D] [-O] [-m [mhvtl|ibm]] [-U] [-Q]

Options:
  -S    Use systemd to manage services inside containers
  -D	wipe database content during initialization phase (database content is kept by default)
  -O	wipe scheduler datastore (objectstore or postgres) content during initialization phase (content is kept by default)
  -a    additional kubernetes resources added to the kubernetes namespace
  -U    Run database unit test only
  -u    Prepare the pods to run the liquibase test
  -T    Execute tests for external tape formats
  -Q    Create the cluster using the last ctageneric image from main
EOF
exit 1
}

die() { echo "$@" 1>&2 ; exit 1; }

while getopts "n:o:d:e:a:p:b:i:B:E:SDOUumTQ" o; do
    case "${o}" in
        o)
            config_schedstore=${OPTARG}
            test -f ${config_schedstore} || error="${error}Scheduler datastore configmap file ${config_schedstore} does not exist\n"
            ;;
        d)
            config_database=${OPTARG}
            test -f ${config_database} || error="${error}Database configmap file ${config_database} does not exist\n"
            ;;
        e)
            config_eos=${OPTARG}
            test -f ${config_eos} || error="${error}EOS configmap file ${config_eos} does not exist\n"
            ;;
        a)
            additional_resources=${OPTARG}
            test -f ${additional_resources} || error="${error}File ${additional_resources} does not exist\n"
            ;;
        m)
            model=${OPTARG}
            if [ "-${model}-" != "-ibm-" ] && [ "-${model}-" != "-mhvtl-" ] ; then error="${error}Library model ${model} does not exist\n"; fi
            ;;
        n)
            instance=${OPTARG}
            ;;
        p)
            pipelineid=${OPTARG}
            ;;
        i)
            dockerimage=${OPTARG}
            ;;
        S)
            usesystemd=1
            ;;
        O)
            keepobjectstore=0
            ;;
        D)
            keepdatabase=0
            ;;
        U)
            runoracleunittests=1
            ;;
        T)
            runexternaltapetests=1
            ;;
        u)
            updatedatabasetest=1
            ;;
        Q)
            systest_only=1
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${instance}" ]; then
    usage
fi

if [ ! -z "${pipelineid}" -a ! -z "${dockerimage}" ]; then
    usage
fi

# everyone needs poddir temporary directory to generate pod yamls
poddir=$(mktemp -d)

# Get Catalogue Schema version
MAJOR=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MAJOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
MINOR=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MINOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
SCHEMA_VERSION="$MAJOR.$MINOR"

# It sets as schema version the previous to the current one to create a database with that schema version
if [ "$updatedatabasetest" == "1" ] ; then
    MIGRATION_FILE=$(find ../../catalogue/cta-catalogue-schema -name "*to${SCHEMA_VERSION}.sql")
    PREVIOUS_SCHEMA_VERSION=$(echo $MIGRATION_FILE | grep -o -E '[0-9]+\.[0-9]' | head -1)
    NEW_SCHEMA_VERSION=$SCHEMA_VERSION
    SCHEMA_VERSION=$PREVIOUS_SCHEMA_VERSION
fi

# We are going to run with repository based images (they have rpms embedded)
if [[ ${systest_only} -eq 1 ]]; then
  COMMITID=$(curl --url "https://gitlab.cern.ch/api/v4/projects/139306/repository/commits" | jq -cr '.[0] | .short_id' | sed -e 's/\(........\).*/\1/')
else
  COMMITID=$(git log -n1 | grep ^commit | cut -d\  -f2 | sed -e 's/\(........\).*/\1/')
fi
if [[ "${systest_only}" -eq 1 ]]; then
  echo "Creating instance from image build for lastest commit on main ${COMMITID}"
  imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${COMMITID} | tail -n1)
elif [ ! -z "${pipelineid}" ]; then
  echo "Creating instance for image built on commit ${COMMITID} with gitlab pipeline ID ${pipelineid}"
  imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${COMMITID} | grep ^${pipelineid}git | sort -n | tail -n1)
  # just a shortcut to avoid time lost checking against the docker registry...
  #imagetag=${pipelineid}git${COMMITID}
elif [ ! -z "${dockerimage}" ]; then
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
echo "Creating instance using docker image with tag: ${imagetag}"

sed -i cta/values.yaml -e "s/ctageneric\:.*/ctageneric:${imagetag}/g"
sed -i init/values.yaml -e "s/ctageneric\:.*/ctageneric:${imagetag}/g"

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

if [ $usesystemd == 1 ] ; then
    echo "Using systemd to start services on some pods"
    for podname in ctafrontend tpsrv ctaeos; do
        sed -i "/^\ *command:/d" ${poddir}/pod-${podname}*.yaml
    done
fi

if [ $keepdatabase == 1 ] ; then
    echo "DB content will be kept"
else
    echo "DB content will be wiped"
fi

if [ $keepobjectstore == 1 ] ; then
    echo "scheduler data store content will be kept"
else
    echo "schedule data store content will be wiped"
fi

echo -n "Creating ${instance} instance "

kubectl create namespace ${instance} || die "FAILED"

# Does kubernetes version supports `get pod --show-all=true`?
# use it for older versions and this is not needed for new versions of kubernetes
KUBECTL_DEPRECATED_SHOWALL=$(kubectl get pod --show-all=true >/dev/null 2>&1 && echo "--show-all=true")
test -z ${KUBECTL_DEPRECATED_SHOWALL} || echo "WARNING: you are running a old version of kubernetes and should think about updating it."

# The CTA registry secret must be copied in the instance namespace to be usable
kubectl get secret ${ctareg_secret} &> /dev/null
if [ $? -eq 0 ]; then
  echo "Copying ${ctareg_secret} secret in ${instance} namespace"
  kubectl get secret ctaregsecret -o yaml | grep -v '^ *namespace:' | kubectl --namespace ${instance} create -f -
fi

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

echo 'copying files to tmpdir'
cp -R ./helm_instances/init ${poddir}
cp -R ./helm_instances/cta ${poddir}
cp ./pod-oracleunittests.yaml ${poddir}

echo  "Setting up init and db pods."
helm install init ${poddir}/init -n ${instance}

echo -n "Waiting for init"
for ((i=0; i<400; i++)); do
  echo -n "."
  kubectl --namespace=${instance} get pod init ${KUBECTL_DEPRECATED_SHOWALL} -o json | jq -r .status.phase | egrep -q 'Succeeded|Failed' && break
  sleep 1
done

echo 'Creating cta instance in ${instance} namespace'
helm dependency build ${poddir}/cta
helm dependency update ${poddir}/cta
helm install cta ${poddir}/cta -n ${instance}


kubectl --namespace=${instance} get pods

# kubectl --namespace=${instance} get pod init ${KUBECTL_DEPRECATED_SHOWALL} -o json | jq -r .status.phase | grep -q Succeeded || die "TIMED OUT"
# echo OK

if [ $runoracleunittests == 1 ] ; then
  echo "Running database unit-tests"
  kubectl create -f ${poddir}/pod-oracleunittests.yaml --namespace=${instance}

  echo -n "Waiting for oracleunittests"
  for ((i=0; i<400; i++)); do
    echo -n "."
    kubectl --namespace=${instance} get pod oracleunittests ${KUBECTL_DEPRECATED_SHOWALL} -o json | jq -r .status.phase | egrep -q 'Succeeded|Failed' && break
    sleep 1
  done
  echo "\n"

  kubectl --namespace=${instance} logs oracleunittests

  # database unit-tests went wrong => exit now with error
  if $(kubectl --namespace=${instance} get pod oracleunittests ${KUBECTL_DEPRECATED_SHOWALL} -o json | jq -r .status.phase | grep -q Failed); then
    echo "oracleunittests pod in Error status here are its last log lines:"
    kubectl --namespace=${instance} logs oracleunittests --tail 10
    die "ERROR: oracleunittests pod in Error state. Initialization failed."
  fi

  # database unit-tests were successful => exit now with success
  exit 0
fi

echo -n "Waiting for all the pods to be in the running state"
for ((i=0; i<240; i++)); do
  echo -n "."
  # exit loop when all pods are in Running state
  kubectl -n ${instance} get pod ${KUBECTL_DEPRECATED_SHOWALL} -o json | jq -r '.items[] | select(.metadata.name != "init") | select(.metadata.name != "oracleunittests") | .status.phase'| grep -q -v Running || break
  sleep 1
done

kubectl get pods -n ${instance}

if [[ $(kubectl -n toto get pod ${KUBECTL_DEPRECATED_SHOWALL} -o json | jq -r '.items[] | select(.metadata.name != "init") | select(.metadata.name != "oracleunittests") | .status.phase'| grep -q -v Running) ]]; then
  echo "TIMED OUT"
  echo "Some pods have not been initialized properly:"
  kubectl --namespace=${instance} get pod ${KUBECTL_DEPRECATED_SHOWALL}
  exit 1
fi
echo OK

kubectl --namespace=${instance} exec ctaeos -- touch /CANSTART

echo -n "Waiting for EOS to be configured"
for ((i=0; i<300; i++)); do
  echo -n "."
  [ "`kubectl --namespace=${instance} exec ctaeos -- bash -c "[ -f /EOSOK ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] && break
  sleep 1
done
[ "`kubectl --namespace=${instance} exec ctaeos -- bash -c "[ -f /EOSOK ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] || die "TIMED OUT"
echo OK




echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${instance} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"

echo -n "Using kinit for ctacli and client"
kubectl --namespace=${instance} exec ctacli -- kinit -kt /root/ctaadmin1.keytab ctaadmin1@TEST.CTA
kubectl --namespace=${instance} exec client -- kinit -kt /root/user1.keytab user1@TEST.CTA



# space=${instance} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
# May be needed for the client to make sure that SSS is not used by default but krb5...
#echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${instance} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
echo OK

echo "klist for client:"
kubectl --namespace=${instance} exec client -- klist


echo "klist for ctacli:"
kubectl --namespace=${instance} exec ctacli -- klist




# Set the workflow rules for archiving, creating tape file replicas in the EOS namespace, retrieving
# files from tape and deleting files.
#
# The FQDN can be set as follows:
# CTA_ENDPOINT=ctafrontend.${instance}.svc.cluster.local:10955
#
# however the simple hostname should be sufficient:
CTA_ENDPOINT=ctafrontend:10955

echo "Setting workflows in namespace ${instance} pod ctaeos:"
CTA_WF_DIR=/eos/${EOSINSTANCE}/proc/cta/workflow
for WORKFLOW in sync::create.default sync::closew.default sync::archived.default sync::archive_failed.default sync::prepare.default sync::abort_prepare.default sync::evict_prepare.default sync::closew.retrieve_written sync::retrieve_failed.default sync::delete.default
do
  echo "eos attr set sys.workflow.${WORKFLOW}=\"proto\" ${CTA_WF_DIR}"
  kubectl --namespace=${instance} exec ctaeos -- bash -c "eos attr set sys.workflow.${WORKFLOW}=\"proto\" ${CTA_WF_DIR}"
done

echo "Instance ${instance} successfully created:"
kubectl --namespace=${instance} get pod ${KUBECTL_DEPRECATED_SHOWALL}

if [ $runexternaltapetests == 1 ] ; then
  echo "Running database unit-tests"
  ./tests/external_tapes_test.sh -n ${instance} -P ${poddir}

  kubectl --namespace=${instance} logs externaltapetests

  # database unit-tests went wrong => exit now with error
  if $(kubectl --namespace=${instance} get pod externaltapetests ${KUBECTL_DEPRECATED_SHOWALL} -o json | jq -r .status.phase | egrep -q 'Failed'); then
    echo "externaltapetests pod in Failed status here are its last log lines:"
    kubectl --namespace=${instance} logs externaltapetests --tail 10
    die "ERROR: externaltapetests pod in Error state. Initialization failed."
  fi

  # database unit-tests were successful => exit now with success
  exit 0
fi

exit 0
