#!/bin/bash

# defaults objectstore to file
config_objectstore="./objectstore-file.yaml"
# defaults DB to sqlite
config_database="./database-sqlite.yaml"
# default library model
model="mhvtl"

# By default keep Database and keep Objectstore
# default should not make user loose data if he forgot the option
keepdatabase=1
keepobjectstore=1

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> [-o <objectstore_configmap>] [-d <database_configmap>] \
      [-p <gitlab pipeline ID> | -b <build tree base> -B <build tree subdir> ]  \
      [-D] [-O] [-m [mhvtl|ibm]]

Options:
  -b    The directory containing both the source and the build tree for CTA. It will be mounted RO in the
        containers.
  -B    The subdirectory within the -b directory where the build tree is.
  -D	wipe database content during initialization phase (database content is kept by default)
  -O	wipe objectstore content during initialization phase (objectstore content is kept by default)
EOF
exit 1
}

die() { echo "$@" 1>&2 ; exit 1; }

while getopts "n:o:d:p:b:B:DOm:" o; do
    case "${o}" in
        o)
            config_objectstore=${OPTARG}
            test -f ${config_objectstore} || error="${error}Objectstore configmap file ${config_objectstore} does not exist\n"
            ;;
        d)
            config_database=${OPTARG}
            test -f ${config_database} || error="${error}Database configmap file ${config_database} does not exist\n"
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
        B)
            buildtreesubdir=${OPTARG}
            ;;
        O)
            keepobjectstore=0
            ;;
        D)
            keepdatabase=0
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

if [ ! -z "${buildtree}" ]; then
    # We need to know the subdir as well
    if [ -z "${buildtreesubdir}" ]; then
      usage
    fi
    # We are going to run with generic images against a build tree.
    echo "Creating instance for build tree in ${buildtree}"

    # Create temporary directory for modified pod files
    poddir=$(mktemp -d)
    cp pod-* ${poddir}
    sed -i ${poddir}/pod-* -e "s/\(^\s\+image\):.*/\1: buildtree-runner\n\1PullPolicy: Never/"

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

    # Create temporary directory for modified pod files
    poddir=$(mktemp -d)
    cp pod-* ${poddir}
    sed -i ${poddir}/pod-* -e "s/\(^\s\+image:[^:]\+:\).*/\1${imagetag}/"

    if [ ! -z "${error}" ]; then
        echo -e "ERROR:\n${error}"
        exit 1
    fi
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

kubectl --namespace ${instance} create configmap init --from-literal=keepdatabase=${keepdatabase} --from-literal=keepobjectstore=${keepobjectstore}

kubectl --namespace ${instance} create configmap buildtree --from-literal=base=${buildtree} --from-literal=subdir=${buildtreesubdir}

echo "creating configmaps in instance"

kubectl create -f ${config_objectstore} --namespace=${instance}
kubectl create -f ${config_database} --namespace=${instance}


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
  kubectl get pod init -a --namespace=${instance} | grep -q Completed && break
  sleep 1
done

# initialization went wrong => exit now with error
kubectl get pod init -a --namespace=${instance} | grep -q Completed || die "TIMED OUT"
echo OK

echo "Launching pods"

for podname in client ctacli tpsrv01 tpsrv02 ctaeos ctafrontend kdc; do
  kubectl create -f ${poddir}/pod-${podname}.yaml --namespace=${instance}
done

echo -n "Waiting for other pods"
for ((i=0; i<240; i++)); do
  echo -n "."
  # exit loop when all pods are in Running state
  kubectl get pods -a --namespace=${instance} | grep -v init | tail -n+2 | grep -q -v Running || break
  sleep 1
done

if [[ $(kubectl get pods -a --namespace=${instance} | grep -v init | tail -n+2 | grep -q -v Running) ]]; then
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
kubectl --namespace=${instance} exec kdc cat /root/admin1.keytab | kubectl --namespace=${instance} exec -i ctacli --  bash -c "cat > /root/admin1.keytab"
kubectl --namespace=${instance} exec kdc cat /root/user1.keytab | kubectl --namespace=${instance} exec -i client --  bash -c "cat > /root/user1.keytab"
kubectl --namespace=${instance} exec kdc cat /root/cta-frontend.keytab | kubectl --namespace=${instance} exec -i ctafrontend --  bash -c "cat > /etc/cta-frontend.keytab"
kubectl --namespace=${instance} exec kdc cat /root/eos.keytab | kubectl --namespace=${instance} exec -i ctaeos --  bash -c "cat > /etc/eos.krb5.keytab"
kubectl --namespace=${instance} exec ctacli -- kinit -kt /root/admin1.keytab admin1@TEST.CTA
kubectl --namespace=${instance} exec client -- kinit -kt /root/user1.keytab user1@TEST.CTA

# create users on the mgm
kubectl --namespace=${instance} exec ctaeos -- groupadd --gid 1100 users
kubectl --namespace=${instance} exec ctaeos -- groupadd --gid 1200 powerusers
kubectl --namespace=${instance} exec ctaeos -- groupadd --gid 1300 ctaadmins
kubectl --namespace=${instance} exec ctaeos -- groupadd --gid 1400 eosadmins
kubectl --namespace=${instance} exec ctaeos -- useradd --uid 11001 --gid 1100 user1
kubectl --namespace=${instance} exec ctaeos -- useradd --uid 12001 --gid 1200 poweruser1
kubectl --namespace=${instance} exec ctaeos -- useradd --uid 13001 --gid 1300 ctaadmin1
kubectl --namespace=${instance} exec ctaeos -- useradd --uid 14001 --gid 1400 eosadmin1


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
for ((i=0; i<300; i++)); do
  echo -n "."
  [ "`kubectl --namespace=${instance} exec ctafrontend -- bash -c "[ -f /etc/ctafrontend_SSS_c.keytab ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] && break
  sleep 1
done
[ "`kubectl --namespace=${instance} exec ctafrontend -- bash -c "[ -f /etc/ctafrontend_SSS_c.keytab ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] || die "TIMED OUT"
kubectl --namespace=${instance} exec ctafrontend -- cat /etc/ctafrontend_SSS_c.keytab | kubectl --namespace=${instance} exec -i ctaeos --  bash -c "cat > /etc/ctafrontend_SSS_c.keytab; chmod 600 /etc/ctafrontend_SSS_c.keytab; chown daemon /etc/ctafrontend_SSS_c.keytab"
echo OK


echo -n "Waiting for EOS to be configured"
for ((i=0; i<300; i++)); do
  echo -n "."
  kubectl --namespace=${instance} logs ctaeos | grep -q  "### ctaeos mgm ready ###" && break
  sleep 1
done
kubectl --namespace=${instance} logs ctaeos | grep -q  "### ctaeos mgm ready ###" || die "TIMED OUT"
echo OK


echo -n "Copying eos SSS on ctacli and client pods to allow recalls"
kubectl --namespace=${instance} exec eos cat /etc/eos.keytab | kubectl --namespace=${instance} exec -i ctacli --  bash -c "cat > /etc/eos.keytab; chmod 600 /etc/eos.keytab"
kubectl --namespace=${instance} exec eos cat /etc/eos.keytab | kubectl --namespace=${instance} exec -i client --  bash -c "cat > /etc/eos.keytab; chmod 600 /etc/eos.keytab"
echo OK


echo "Instance ${instance} successfully created:"
kubectl get pods -a --namespace=${instance}

exit 0
