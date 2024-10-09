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

catalogue_config=presets/dev-catalogue-postgres-values.yaml
scheduler_config=presets/dev-scheduler-file-values.yaml

# default library model
model="mhvtl"

# EOS short instance name
eos_instance=ctaeos

# By default to not use systemd to manage services inside the containers
usesystemd=0

# By default keep Database and keep Scheduler datastore data
# default should not make user loose data if he forgot the option
wipe_catalogue=false
wipe_scheduler=false

# By default run the standard test no oracle dbunittests
runoracleunittests=0

# By default doesn't prepare the images with the previous schema version
updatedatabasetest=0

# Create an instance for
systest_only=0

# Number of tape servers to spawn by default
tpsrv_count=2
# for the pod images
registry_host="gitlab-registry.cern.ch/cta"

generate_library_config() {
  local target_file="$1"
  local library_device="$2"
  local lsscsi_g="$(lsscsi -g)"
  
  local line=$(echo "$lsscsi_g" | grep "${library_device}")
  local scsi_host="$(echo "$line" | sed -e 's/^.//' | cut -d\: -f1)"
  local scsi_channel="$(echo "$line" | cut -d\: -f2)"
  
  # Get drive names and drive devices
  local drivenames=$(echo "$lsscsi_g" | grep "^.${scsi_host}:${scsi_channel}:" | grep tape | sed -e 's/^.[0-9]\+:[0-9]\+:\([0-9]\+\):\([0-9]\+\)\].*/VDSTK\1\2/' | xargs -itoto echo -n " toto")
  local drivedevices=$(echo "$lsscsi_g" | grep "^.${scsi_host}:${scsi_channel}:" | grep tape | awk '{print $6}' | sed -e 's%/dev/%n%' | xargs -itoto echo -n " toto")  
  
  # Get the tapes currently in the library
  local tapes=$(mtx -f "/dev/${library_device}" status | grep Storage\ Element | grep Full | sed -e 's/.*VolumeTag=//;s/ //g;s/\(......\).*/\1/' | xargs -itoto echo -n " toto" | sed -e 's/^ //')

  cat <<EOF > "$target_file"
tpsrv:
  library:
    type: "mhvtl"
    name: "$(echo ${line} | awk '{print $4}')"
    device: "${library_device}"
    drivenames:
$(for name in ${drivenames}; do echo "      - \"${name}\""; done)
    drivedevices:
$(for device in ${drivedevices}; do echo "      - \"${device}\""; done)
    tapes:
$(for tape in ${tapes}; do echo "      - \"${tape}\""; done)
EOF
}


die() { echo "$@" 1>&2 ; exit 1; }

usage() {
  echo "Script for managing Kubernetes resources."
  echo ""
  echo "Usage: $0 -n <namespace> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                     Shows help output."
  echo "  -n <namespace>:                 Specify the Kubernetes namespace."
  echo "  -o <schedstore_configmap>:      Path to the scheduler configuration values file."
  echo "  -d <database_configmap>:        Path to the catalogue configuration values file."
  echo "  -l <library_configmap>:         Path to the library configuration values file. If not provided, this file will be auto-generated"
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

while getopts "n:o:d:p:b:i:r:c:l:SDOUumQ" o; do
  case "${o}" in
    o)
      scheduler_config=${OPTARG} ;;
    d)
      catalogue_config=${OPTARG} ;;
    l)
      library_config=${OPTARG} ;;
    m)
      model=${OPTARG} ;;
    n)
      namespace=${OPTARG} ;;
    p)
      pipelineid=${OPTARG} ;;
    i)
      dockerimage=${OPTARG} ;;
    r)
      registry_host=${OPTARG} ;;
    c)
      tpsrv_count=${OPTARG} ;;
    S)
      usesystemd=1 ;;
    O)
      wipe_scheduler=true ;;
    D)
      wipe_catalogue=true ;;
    U)
      runoracleunittests=1 ;;
    u)
      updatedatabasetest=1 ;;
    Q)
      systest_only=1 ;;
    *)
      echo "Unsupported argument: $1"
      usage ;;
  esac
done
shift $((OPTIND-1))

if [ -z "${namespace}" ]; then
  echo "Missing mandatory argument: -n"
  usage
fi

if [ -n "${pipelineid}" ] && [ -n "${dockerimage}" ]; then
  echo "Missing mandatory argument. Please provide either a pipeline id or a docker image"
  usage
fi

test -f "${scheduler_config}" || die "ERROR: Scheduler config file ${scheduler_config} does not exist\n"
test -f "${catalogue_config}" || die "ERROR: Catalgue config file ${catalogue_config} does not exist\n"
if [ "-${model}-" != "-ibm-" ] && [ "-${model}-" != "-mhvtl-" ] ; then die "ERROR: Library model ${model} does not exist\n"; fi

if ! command -v helm >/dev/null 2>&1; then
  die "ERROR: Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
fi

# TODO: the user should just provide the image details and this logic should be elsewhere; makes the whole thing very confusing initially
# We are going to run with repository based images (they have rpms embedded)
../ci_helpers/get_registry_credentials.sh --check > /dev/null || { echo "Error: Credential check failed"; exit 1; }
if [[ ${systest_only} -eq 1 ]]; then
  commit_id=$(curl --url "https://gitlab.cern.ch/api/v4/projects/139306/repository/commits" | jq -cr '.[0] | .short_id' | sed -e 's/\(........\).*/\1/')
else
  commit_id=$(git log -n1 | grep ^commit | cut -d\  -f2 | sed -e 's/\(........\).*/\1/')
fi
if [[ "${systest_only}" -eq 1 ]]; then
  echo "Creating instance from image build for lastest commit on main ${commit_id}"
  imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${commit_id} | tail -n1)
elif [ -n "${pipelineid}" ]; then
  echo "Creating instance for image built on commit ${commit_id} with gitlab pipeline ID ${pipelineid}"
  imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${commit_id} | grep ^${pipelineid}git | sort -n | tail -n1)
  # just a shortcut to avoid time lost checking against the docker registry...
  #imagetag=${pipelineid}git${commit_id}
elif [ -n "${dockerimage}" ]; then
  echo "Creating instance for image ${dockerimage}"
  imagetag=${dockerimage}
else
  echo "Creating instance for latest image built for ${commit_id} (highest PIPELINEID)"
  imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${commit_id} | sort -n | tail -n1)
fi
if [ "${imagetag}" == "" ]; then
  die "ERROR: commit ${commit_id} has no docker image available in gitlab registry, please check pipeline status and registry images available."
fi

if [ $usesystemd == 1 ] ; then
  echo "Using systemd to start services on some pods"
  # TODO: this shouldn't be happening by a find-replace, but rather by updating the values.yml
  # E.g. either setting an option or using a different values.yml
  # Additionally, this doesn't even work in the first place as these files don't exist anymore
  # for podname in ctafrontend tpsrv ctaeos; do
  #     sed -i "/^\ *command:/d" ${poddir}/pod-${podname}*.yaml
  # done
  die "ERROR: systemd support has not yet been implement in the Helm setup"
fi

if [ "$wipe_catalogue" == "true" ] ; then
  echo "Catalogue content will be wiped"
else
  echo "Catalogue content will be kept"
fi

if [ "$wipe_scheduler" == "true" ]; then
  echo "scheduler data store content will be wiped"
else
  echo "scheduler data store content will be kept"
fi


# Grab a unique library
devices_all=$(lsscsi -g | grep mediumx | awk {'print $7'} | sed -e 's%/dev/%%' | sort)
devices_in_use=$(kubectl get all --all-namespaces -l cta/library-device -o jsonpath='{.items[*].metadata.labels.cta/library-device}' | tr ' ' '\n' | sort | uniq)
unused_devices=$(comm -23 <(echo "$devices_all") <(echo "$devices_in_use"))
if [ -z "$unused_devices" ]; then
  die "ERROR: No unused library devices available. All the following libraries are in use: $devices_in_use"
fi

if [ -z "${library_config}" ]; then
  echo "Library configuration not provided. Auto-generating..."
  # Ensure the temporary file is deleted on script exit or interruption
  library_config=$(mktemp)
  trap 'rm -f "$library_config"' EXIT

  library_device=$(echo "$unused_devices" | head -n 1)
  generate_library_config $library_config $library_device
  echo "---"
  cat $library_config
  echo "---"
else
  # See what device was provided in the values and check that it is not in use
  library_device=$(awk '/device:/ {gsub("\"","",$2); print $2}' $library_config)
  if ! echo "$unused_devices" | grep -qw "$library_device"; then
    die "ERROR: provided library config specifies a device that is already in use: $library_device"
  fi
fi
echo "Using library device: ${library_device}"

# Determine the library config to use

echo "Creating ${namespace} namespace"
kubectl create namespace ${namespace}
# TODO: eventually we will need to copy multiple secrets as there are multiple registries
# This works fine if the secret contains a service account, but will not work with access tokens.
# The registry secret(s) must be copied in the namespace to be usable
kubectl get secret ${ctareg_secret} &> /dev/null
if [ $? -eq 0 ]; then
  echo "Copying ${ctareg_secret} secret in ${namespace} namespace"
  kubectl get secret ctaregsecret -o yaml | grep -v '^ *namespace:' | kubectl --namespace ${namespace} create -f -
fi

catalogue_opts=""
if [ $runoracleunittests == 1 ] ; then
  catalogue_opts+=" --set oracleUnitTests.enabled=true"
fi
# Get Catalogue Schema version
catalogue_major_ver=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MAJOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
catalogue_minor_ver=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MINOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
catalogue_schema_version="$catalogue_major_ver.$catalogue_minor_ver"
if [[ "$updatedatabasetest" == "1" ]] ; then
  migration_files=$(find ../../catalogue/cta-catalogue-schema -name "*to${catalogue_schema_version}.sql")
  prev_catalogue_schema_version=$(echo "$migration_files" | grep -o -E '[0-9]+\.[0-9]' | head -1)
  # TODO: fill in the catalogue-opts in here with the correct prev version and everything
  catalogue_schema_version=$prev_catalogue_schema_version
  echo "Deploying with previous catalogue schema version: ${catalogue_schema_version}"
else
  echo "Deploying with current catalogue schema version: ${catalogue_schema_version}"
fi

echo  "Installing cataloguedb chart..."
set -x
helm install cataloguedb helm/cataloguedb --namespace ${namespace} \
                                          --set catalogue.schemaVersion="${catalogue_schema_version}" \
                                          --values "${catalogue_config}" \
                                          --wait --timeout 5m \
                                          ${catalogue_opts}
set +x

echo  "Installing init chart..."
set -x
helm install init helm/init --namespace ${namespace} \
                            --set global.image.registry="${registry_host}" \
                            --set global.image.tag="${imagetag}" \
                            --set catalogue.schemaVersion="${catalogue_schema_version}" \
                            --set wipeCatalogue=${wipe_catalogue} \
                            --set wipeScheduler=${wipe_scheduler} \
                            --values "${library_config}" \
                            --values "${scheduler_config}" \
                            --values "${catalogue_config}" \
                            --wait --wait-for-jobs --timeout 5m
set +x

echo ""
echo "Processing dependencies of cta chart..."
helm dependency update helm/cta
echo "Installing cta chart..."
set -x
helm install cta helm/cta --namespace ${namespace} \
                          --set global.image.registry="${registry_host}" \
                          --set global.image.tag="${imagetag}" \
                          --set tpsrv.tpsrv.replicaCount=${tpsrv_count} \
                          --values "${library_config}" \
                          --values "${scheduler_config}" \
                          --values "${catalogue_config}" \
                          --wait --timeout 5m
set +x

# TODO: the following part is configuration that is not (and should not) be part of the Helm setup.
# Eventually, the user should be able to provide a setup script that will be executed here.


# Set up kerberos
echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${namespace} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
echo -n "Using kinit for ctacli and client"
kubectl --namespace=${namespace} exec ctacli -- kinit -kt /root/ctaadmin1.keytab ctaadmin1@TEST.CTA
kubectl --namespace=${namespace} exec client -- kinit -kt /root/user1.keytab user1@TEST.CTA
# space=${namespace} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
# May be needed for the client to make sure that SSS is not used by default but krb5...
#echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${namespace} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
echo "klist for client:"
kubectl --namespace=${namespace} exec client -- klist
echo "klist for ctacli:"
kubectl --namespace=${namespace} exec ctacli -- klist


# Set the workflow rules for archiving, creating tape file replicas in the EOS namespace, retrieving
# files from tape and deleting files.
echo "Setting workflows in namespace ${namespace} pod ctaeos:"
cta_workflow_dir=/eos/${eos_instance}/proc/cta/workflow
for workflow in sync::create.default sync::closew.default sync::archived.default sync::archive_failed.default sync::prepare.default sync::abort_prepare.default sync::evict_prepare.default sync::closew.retrieve_written sync::retrieve_failed.default sync::delete.default; do
  echo "eos attr set sys.workflow.${workflow}=\"proto\" ${cta_workflow_dir}"
  kubectl --namespace=${namespace} exec ctaeos -- bash -c "eos attr set sys.workflow.${workflow}=\"proto\" ${cta_workflow_dir}"
done

echo "Instance ${namespace} successfully created:"
kubectl --namespace=${namespace} get pods
