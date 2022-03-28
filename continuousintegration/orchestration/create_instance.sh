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

# CTA registry secret name
ctareg_secret='ctaregsecret'

# defaults objectstore to file
config_objectstore="./objectstore-file.yaml"
# defaults DB to sqlite
config_database="./database-sqlite.yaml"
# default library model
model="mhvtl"
# defaults MGM namespace to inMemory
config_eos="./eos-config-inmemory.yaml"
# shared configmap for eoscta instance
config_eoscta="./eoscta-config.yaml"

# EOS short instance name
EOSINSTANCE=ctaeos

# By default to not use systemd to manage services inside the containers
usesystemd=0

# By default keep Database and keep Objectstore
# default should not make user loose data if he forgot the option
keepdatabase=1
keepobjectstore=1

# By default run the standard test no oracle dbunittests
runoracleunittests=0

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> [-o <objectstore_configmap>] [-d <database_configmap>] \
      [-e <eos_configmap>] [-a <additional_k8_resources>]\
      [-p <gitlab pipeline ID> | -b <build tree base> -B <CTA build tree subdir> [-E <EOS build tree subdir>]] \
      [-S] [-D] [-O] [-m [mhvtl|ibm]] [-U]

Options:
  -S    Use systemd to manage services inside containers
  -b    The directory containing both the source and the build tree for CTA (and possibly EOS). It will be mounted RO in the
        containers.
  -B    The subdirectory within the -b directory where the CTA build tree is.
  -E    The subdirectory within the -b directory where the EOS build tree is.
  -D	wipe database content during initialization phase (database content is kept by default)
  -O	wipe objectstore content during initialization phase (objectstore content is kept by default)
  -a    additional kubernetes resources added to the kubernetes namespace
  -U    Run database unit test only
EOF
exit 1
}

die() { echo "$@" 1>&2 ; exit 1; }

while getopts "n:o:d:e:a:p:b:B:E:SDOUm:" o; do
    case "${o}" in
        o)
            config_objectstore=${OPTARG}
            test -f ${config_objectstore} || error="${error}Objectstore configmap file ${config_objectstore} does not exist\n"
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
        b)
            buildtree=${OPTARG}
            ;;
        S)
            usesystemd=1
            ;;
        B)
            ctabuildtreesubdir=${OPTARG}
            ;;
        E)
            eosbuildtreesubdir=${OPTARG}
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
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${instance}" ]; then
    usage
fi

if [ ! -z "${pipelineid}" -a ! -z "${buildtree}" ]; then
    usage
fi

# everyone needs poddir temporary directory to generate pod yamls
poddir=$(mktemp -d)

if [ ! -z "${buildtree}" ]; then
    # We need to know the subdir as well
    if [ -z "${ctabuildtreesubdir}" ]; then
      usage
    fi
    # We are going to run with generic images against a build tree.
    echo "Creating instance for build tree in ${buildtree}"

    # tag image as otherwise kubernetes will always pull latest and won't find it...
    docker rmi buildtree-runner:v0 &>/dev/null
    docker tag buildtree-runner buildtree-runner:v0
    cp pod-* ${poddir}
    if [ -z "${eosbuildtreesubdir}" ]; then
      sed -i ${poddir}/pod-* -e "s/\(^\s\+image\):.*/\1: buildtree-runner:latest\n\1PullPolicy: Never/"
    else
      sed -i ${poddir}/pod-* -e "s/\(^\s\+image\):.*/\1: doublebuildtree-runner:latest\n\1PullPolicy: Never/"
    fi

    # Add the build tree mount point the pods (volume mount then volume).
    sed -i ${poddir}/pod-* -e "s|\(^\s\+\)\(volumeMounts:\)|\1\2\n\1- mountPath: ${buildtree}\n\1  name: buildtree\n\1  readOnly: true|"
    sed -i ${poddir}/pod-* -e "s|\(^\s\+\)\(volumes:\)|\1\2\n\1- name: buildtree\n\1  hostPath:\n\1    path: ${buildtree}|"

    if [ ! -z "${error}" ]; then
        echo -e "ERROR:\n${error}"
        exit 1
    fi
else
    # We are going to run with repository based images (they have rpms embedded)
    COMMITID=$(git log -n1 | grep ^commit | cut -d\  -f2 | sed -e 's/\(........\).*/\1/')
    if [ -z "${pipelineid}" ]; then
      echo "Creating instance for latest image built for ${COMMITID} (highest PIPELINEID)"
      imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${COMMITID} | sort -n | tail -n1)
    else
      echo "Creating instance for image built on commit ${COMMITID} with gitlab pipeline ID ${pipelineid}"
      imagetag=$(../ci_helpers/list_images.sh 2>/dev/null | grep ${COMMITID} | grep ^${pipelineid}git | sort -n | tail -n1)
    fi
    if [ "${imagetag}" == "" ]; then
      echo "commit:${COMMITID} has no docker image available in gitlab registry, please check pipeline status and registry images available."
      exit 1
    fi
    echo "Creating instance using docker image with tag: ${imagetag}"

    cp pod-* ${poddir}
    sed -i ${poddir}/pod-* -e "s/\(^\s\+image:[^:]\+:\).*/\1${imagetag}/"

    if [ ! -z "${error}" ]; then
        echo -e "ERROR:\n${error}"
        exit 1
    fi
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
    echo "objecstore content will be kept"
else
    echo "objectstore content will be wiped"
fi


echo -n "Creating ${instance} instance "

kubectl create namespace ${instance} || die "FAILED"

# The CTA registry secret must be copied in the instance namespace to be usable
kubectl get secret ${ctareg_secret} &> /dev/null
if [ $? -eq 0 ]; then
  echo "Copying ${ctareg_secret} secret in ${instance} namespace"
  kubectl get secret ctaregsecret -o yaml | grep -v '^ *namespace:' | kubectl --namespace ${instance} create -f -
fi

kubectl --namespace ${instance} create configmap init --from-literal=keepdatabase=${keepdatabase} --from-literal=keepobjectstore=${keepobjectstore}

kubectl --namespace ${instance} create configmap buildtree --from-literal=base=${buildtree} --from-literal=cta_subdir=${ctabuildtreesubdir} \
  --from-literal=eos_subdir=${eosbuildtreesubdir}

if [ ! -z "${additional_resources}" ]; then
  kubectl --namespace ${instance} create -f ${additional_resources} || die "Could not create additional resources described in ${additional_resources}"
  kubectl --namespace ${instance} get pods -a
fi

echo "creating configmaps in instance"

kubectl create -f ${config_objectstore} --namespace=${instance}
kubectl create -f ${config_database} --namespace=${instance}
kubectl create -f ${config_eos} --namespace=${instance}
kubectl create -f ${config_eoscta} --namespace=${instance}

echo -n "Requesting an unused ${model} library"
kubectl create -f ./pvc_library_${model}.yaml --namespace=${instance}
for ((i=0; i<120; i++)); do
  echo -n "."
  kubectl get persistentvolumeclaim claimlibrary --namespace=${instance} | grep -q Bound && break
  sleep 1
done
kubectl get persistentvolumeclaim claimlibrary --namespace=${instance} | grep -q Bound || die "TIMED OUT"
echo "OK"
LIBRARY_DEVICE=$(kubectl get persistentvolumeclaim claimlibrary --namespace=${instance} -o yaml| grep -i volumeName | sed -e 's%.*sg%sg%')

kubectl --namespace=${instance} create -f /opt/kubernetes/CTA/library/config/library-config-${LIBRARY_DEVICE}.yaml

echo "Got library: ${LIBRARY_DEVICE}"

echo "Requesting an unused log volume"
kubectl create -f ./pvc_logs.yaml --namespace=${instance}

echo "Requesting an unused stg volume"
kubectl create -f ./pvc_stg.yaml --namespace=${instance}

echo "Creating services in instance"

for service_file in *svc\.yaml; do
  kubectl create -f ${service_file} --namespace=${instance}
done


echo "Creating pods in instance"

kubectl	create -f ${poddir}/pod-init.yaml --namespace=${instance}

echo -n "Waiting for init"
for ((i=0; i<400; i++)); do
  echo -n "."
  kubectl get pod init -a --namespace=${instance} | egrep -q 'Completed|Error' && break
  sleep 1
done

# initialization went wrong => exit now with error
if $(kubectl get pod init -a --namespace=${instance} | grep -q Error); then
	echo "init pod in Error status here are its last log lines:"
	kubectl --namespace=${instance} logs init --tail 10
	die "ERROR: init pod in ErERROR: init pod in Error state. Initialization failed."
fi

kubectl get pod init -a --namespace=${instance} | grep -q Completed || die "TIMED OUT"
echo OK

if [ $runoracleunittests == 1 ] ; then
  echo "Running database unit-tests"
  kubectl create -f ${poddir}/pod-oracleunittests.yaml --namespace=${instance}

  echo -n "Waiting for oracleunittests"
  for ((i=0; i<400; i++)); do
    echo -n "."
    kubectl get pod oracleunittests -a --namespace=${instance} | egrep -q 'Completed|Error' && break
    sleep 1
  done

  kubectl --namespace=${instance} logs oracleunittests

  # database unit-tests went wrong => exit now with error
  if $(kubectl get pod oracleunittests -a --namespace=${instance} | grep -q Error); then
    echo "init pod in Error status here are its last log lines:"
    kubectl --namespace=${instance} logs oracleunittests --tail 10
    die "ERROR: oracleunittests pod in Error state. Initialization failed."
  fi

  # database unit-tests were successful => exit now with success
  exit 0
fi


echo "Launching pods"

for podname in client ctacli tpsrv01 tpsrv02 ctaeos ctafrontend kdc; do
  kubectl create -f ${poddir}/pod-${podname}.yaml --namespace=${instance}
done

echo -n "Waiting for other pods"
for ((i=0; i<240; i++)); do
  echo -n "."
  # exit loop when all pods are in Running state
  kubectl get pods -a --namespace=${instance} | grep -v init | grep -v oracleunittests | tail -n+2 | grep -q -v Running || break
  sleep 1
done

if [[ $(kubectl get pods -a --namespace=${instance} | grep -v init | grep -v oracleunittests | tail -n+2 | grep -q -v Running) ]]; then
  echo "TIMED OUT"
  echo "Some pods have not been initialized properly:"
  kubectl get pods -a --namespace=${instance}
  exit 1
fi
echo OK

echo -n "Waiting for KDC to be configured"
# Kdc logs sometimes get truncated. We rely on a different mechanism to detect completion
for ((i=0; i<300; i++)); do
  echo -n "."
  [ "`kubectl --namespace=${instance} exec kdc -- bash -c "[ -f /root/kdcReady ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] && break
  sleep 1
done

[ "`kubectl --namespace=${instance} exec kdc -- bash -c "[ -f /root/kdcReady ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] || die "TIMED OUT"
echo OK

echo -n "Configuring KDC clients (frontend, cli...) "
kubectl --namespace=${instance} exec kdc cat /etc/krb5.conf | kubectl --namespace=${instance} exec -i client --  bash -c "cat > /etc/krb5.conf"
kubectl --namespace=${instance} exec kdc cat /etc/krb5.conf | kubectl --namespace=${instance} exec -i ctacli --  bash -c "cat > /etc/krb5.conf"
kubectl --namespace=${instance} exec kdc cat /etc/krb5.conf | kubectl --namespace=${instance} exec -i ctafrontend --  bash -c "cat > /etc/krb5.conf"
kubectl --namespace=${instance} exec kdc cat /etc/krb5.conf | kubectl --namespace=${instance} exec -i ctaeos --  bash -c "cat > /etc/krb5.conf"
kubectl --namespace=${instance} exec kdc cat /root/ctaadmin1.keytab | kubectl --namespace=${instance} exec -i ctacli --  bash -c "cat > /root/ctaadmin1.keytab"
kubectl --namespace=${instance} exec kdc cat /root/user1.keytab | kubectl --namespace=${instance} exec -i client --  bash -c "cat > /root/user1.keytab"
# need to mkdir /etc/cta folder as cta rpm may not already be installed (or put it somewhere else and move it later???)
kubectl --namespace=${instance} exec kdc cat /root/cta-frontend.keytab | kubectl --namespace=${instance} exec -i ctafrontend --  bash -c "mkdir -p /etc/cta; cat > /etc/cta/cta-frontend.krb5.keytab"
kubectl --namespace=${instance} exec kdc cat /root/eos-server.keytab | kubectl --namespace=${instance} exec -i ctaeos --  bash -c "cat > /etc/eos-server.krb5.keytab"
kubectl --namespace=${instance} exec ctacli -- kinit -kt /root/ctaadmin1.keytab ctaadmin1@TEST.CTA
kubectl --namespace=${instance} exec client -- kinit -kt /root/user1.keytab user1@TEST.CTA

## THE FILE IS MOVED THERE MUCH LATER AND OVERWRITES THIS
# THIS HAS TO BE IMPROVED (DEFINITELY) SO THAT WE CAN ASYNCHRONOUSLY UPDATE THE CONFIGURATION FILES...
# SYSTEMD IS THE WAY TO GO
# Add this for SSI prococol buffer workflow (xrootd >=4.8.2)
#echo "mgmofs.protowfendpoint ctafrontend:10955" | kubectl --namespace=${instance} exec -i ctaeos -- bash -c "cat >> /etc/xrd.cf.mgm"
#echo "mgmofs.protowfresource /ctafrontend" | kubectl --namespace=${instance} exec -i ctaeos -- bash -c "cat >> /etc/xrd.cf.mgm"
#echo "mgmofs.tapeenabled true" | kubectl --namespace=${instance} exec -i ctaeos -- bash -c "cat >> /etc/xrd.cf.mgm"


# allow eos to start
kubectl --namespace=${instance} exec ctaeos -- touch /CANSTART


# create users on the mgm
# this is done in ctaeos-mgm.sh as the mgm needs this to setup the ACLs

# use krb5 and then unix fod xrootd protocol on the client pod for eos, xrdcp and cta everything should be fine!
echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${instance} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
# May be needed for the client to make sure that SSS is not used by default but krb5...
#echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${instance} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
echo OK

echo "klist for client:"
kubectl --namespace=${instance} exec client klist

echo "klist for ctacli:"
kubectl --namespace=${instance} exec ctacli klist


echo -n "Configuring cta SSS for ctafrontend access from ctaeos"
for ((i=0; i<1000; i++)); do
  echo -n "."
  [ "`kubectl --namespace=${instance} exec ctaeos -- bash -c "[ -f /etc/eos.keytab ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] && break
  sleep 1
done
[ "`kubectl --namespace=${instance} exec ctaeos -- bash -c "[ -f /etc/eos.keytab ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] || die "TIMED OUT"
kubectl --namespace=${instance} exec ctaeos -- grep ${EOSINSTANCE} /etc/eos.keytab | sed "s/daemon/${EOSINSTANCE}/g" |\
kubectl --namespace=${instance} exec -i ctafrontend -- \
bash -c "cat > /etc/cta/eos.sss.keytab.tmp; chmod 400 /etc/cta/eos.sss.keytab.tmp; chown cta /etc/cta/eos.sss.keytab.tmp; mv /etc/cta/eos.sss.keytab.tmp /etc/cta/eos.sss.keytab"
echo OK

echo -n "Waiting for EOS to be configured"
for ((i=0; i<300; i++)); do
  echo -n "."
  [ "`kubectl --namespace=${instance} exec ctaeos -- bash -c "[ -f /EOSOK ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] && break
  sleep 1
done
[ "`kubectl --namespace=${instance} exec ctaeos -- bash -c "[ -f /EOSOK ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] || die "TIMED OUT"
echo OK


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


echo -n "Copying eos SSS on ctacli and client pods to allow recalls"
kubectl --namespace=${instance} exec ctaeos cat /etc/eos.keytab | kubectl --namespace=${instance} exec -i ctacli --  bash -c "cat > /etc/eos.keytab; chmod 600 /etc/eos.keytab"
kubectl --namespace=${instance} exec ctaeos cat /etc/eos.keytab | kubectl --namespace=${instance} exec -i client --  bash -c "cat > /etc/eos.keytab; chmod 600 /etc/eos.keytab"
echo OK


echo -n "Waiting for cta-frontend to be Ready"
for ((i=0; i<300; i++)); do
  echo -n "."
  kubectl --namespace=${instance} exec ctafrontend -- bash -c 'test -f /var/log/cta/cta-frontend.log && grep -q "cta-frontend started" /var/log/cta/cta-frontend.log' && break
  sleep 1
done
kubectl --namespace=${instance} exec ctafrontend -- bash -c 'grep -q "cta-frontend started" /var/log/cta/cta-frontend.log' || die "TIMED OUT"
echo OK


echo "Instance ${instance} successfully created:"
kubectl get pods -a --namespace=${instance}

exit 0
