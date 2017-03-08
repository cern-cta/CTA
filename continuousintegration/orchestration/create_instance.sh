#!/bin/bash

# defaults objectstore to file
config_objectstore="./objectstore-file.yaml"
# defaults DB to sqlite
config_database="./database-sqlite.yaml"

# By default keep Database and keep Objectstore
# default should not make user loose data if he forgot the option
keepdatabase=1
keepobjectstore=1

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> [-o <objectstore_configmap>] [-d <database_configmap>] [-p <gitlab pipeline ID>] [-D] [-O]

Options:
  -D	wipe database content during initialization phase (database content is kept by default)
  -O	wipe objectstore content during initialization phase (objectstore content is kept by default)
EOF
exit 1
}

while getopts "n:o:d:p:DO" o; do
    case "${o}" in
        o)
            config_objectstore=${OPTARG}
            test -f ${config_objectstore} || error="${error}Objectstore configmap file ${config_objectstore} does not exist\n"
            ;;
        d)
            config_database=${OPTARG}
            test -f ${config_database} || error="${error}Database configmap file ${config_database} does not exist\n"
            ;;
        n)
            instance=${OPTARG}
            ;;
	p)
            pipelineid=${OPTARG}
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

kubectl create namespace ${instance} || (echo "FAILED"; exit 1)

kubectl --namespace ${instance} create configmap init --from-literal=keepdatabase=${keepdatabase} --from-literal=keepobjectstore=${keepobjectstore}


echo "creating configmaps in instance"

kubectl create -f ${config_objectstore} --namespace=${instance}
kubectl create -f ${config_database} --namespace=${instance}


echo -n "Requesting an unused MHVTL library"
kubectl create -f /opt/kubernetes/CTA/library/library_claim.yaml --namespace=${instance}
for ((i=0; i<120; i++)); do
  echo -n "."
  kubectl get persistentvolumeclaim claimlibrary --namespace=${instance} | grep -q Bound && break
  sleep 1
done
kubectl get persistentvolumeclaim claimlibrary --namespace=${instance} | grep -q Bound || (echo "TIMED OUT"; exit 1)
echo "OK"
LIBRARY_DEVICE=$(kubectl get persistentvolumeclaim claimlibrary --namespace=${instance} -o yaml| grep -i volumeName | sed -e 's%.*sg%sg%')

kubectl --namespace=${instance} create -f /opt/kubernetes/CTA/library/config/library-config-${LIBRARY_DEVICE}.yaml

echo "Got library: ${LIBRARY_DEVICE}"

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
kubectl get pod init -a --namespace=${instance} | grep -q Completed || (echo "TIMED OUT"; exit 1)
echo OK

echo "Launching pods"

for podname in ctacli tpsrv ctaeos ctafrontend kdc; do
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

[ "`kubectl --namespace=${instance} exec kdc -- bash -c "[ -f /root/kdcReady ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] || (echo "TIMED OUT"; echo "Failed to configure KDC."; exit 1)
echo OK

echo -n "Configuring KDC clients (frontend, cli...) "
kubectl --namespace=${instance} exec kdc cat /etc/krb5.conf | kubectl --namespace=${instance} exec -i ctacli --  bash -c "cat > /etc/krb5.conf" 
kubectl --namespace=${instance} exec kdc cat /etc/krb5.conf | kubectl --namespace=${instance} exec -i ctafrontend --  bash -c "cat > /etc/krb5.conf"
kubectl --namespace=${instance} exec kdc cat /root/admin1.keytab | kubectl --namespace=${instance} exec -i ctacli --  bash -c "cat > /root/admin1.keytab"
kubectl --namespace=${instance} exec kdc cat /root/cta-frontend.keytab | kubectl --namespace=${instance} exec -i ctafrontend --  bash -c "cat > /etc/cta-frontend.keytab"
kubectl --namespace=${instance} exec ctacli -- kinit -kt /root/admin1.keytab admin1@TEST.CTA
echo OK

echo "klist for ctacli:"
kubectl --namespace=${instance} exec ctacli klist


echo -n "Configuring cta SSS for ctafrontend access from ctaeos"
for ((i=0; i<300; i++)); do
  echo -n "."
  [ "`kubectl --namespace=${instance} exec ctafrontend -- bash -c "[ -f /etc/ctafrontend_SSS_c.keytab ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] && break
  sleep 1
done
[ "`kubectl --namespace=${instance} exec ctafrontend -- bash -c "[ -f /etc/ctafrontend_SSS_c.keytab ] && echo -n Ready || echo -n Not ready"`" = "Ready" ] || (echo "TIMED OUT"; echo "Failed to retrieve cta frontend SSS key"; exit 1)
kubectl --namespace=${instance} exec ctafrontend -- cat /etc/ctafrontend_SSS_c.keytab | kubectl --namespace=${instance} exec -i ctaeos --  bash -c "cat > /etc/ctafrontend_SSS_c.keytab; chmod 600 /etc/ctafrontend_SSS_c.keytab; chown daemon /etc/ctafrontend_SSS_c.keytab"
echo OK


echo -n "Waiting for EOS to be configured"
for ((i=0; i<300; i++)); do
  echo -n "."
  kubectl --namespace=${instance} logs ctaeos | grep -q  "### ctaeos mgm ready ###" && break
  sleep 1
done
kubectl --namespace=${instance} logs ctaeos | grep -q  "### ctaeos mgm ready ###" || (echo "TIMED OUT"; exit 1)
echo OK

echo "Instance ${instance} successfully created:"
kubectl get pods -a --namespace=${instance}

exit 0
