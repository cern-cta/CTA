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

# makes sure that set -e from gitlab CI scripts is not enabled
set +e
# enable pipefail to get the return code of a failed command in pipeline and not the return code of the last command in pipeline:
# http://stackoverflow.com/questions/6871859/piping-command-output-to-tee-but-also-save-exit-code-of-command
set -o pipefail

# global variable to capture the return code of the last command executed in execute_log function
execute_log_rc=0
# orchestration directory so that we can come back here and launch delete_instance during cleanup
orchestration_dir=${PWD}
# keep or drop namespace after systemtest_script? By default drop it.
keepnamespace=0
# by default use sqlite DB
useoracle=0
# by default use VFS objectstore
useceph=0
# by default do not use systemd to manage services in containers
usesystemd=0
# time out for the kubernetes eoscta instance creation
CREATEINSTANCE_TIMEOUT=1400
# preflight test script
PREFLIGHTTEST_SCRIPT='tests/preflighttest.sh'
# default preflight checks timeout is 60 seconds
PREFLIGHTTEST_TIMEOUT=60
# default systemtest timeout is 1 hour
SYSTEMTEST_TIMEOUT=3600
# by default do not cleanup leftover namespaces
cleanup_namespaces=0
# By default assume that ORACLE_SUPPORT is ON
# if this script is running outside of gitlab CI
test -z ${ORACLE_SUPPORT+x} && ORACLE_SUPPORT="ON"
# By default assume that SCHED_TYPE is "objectstore"
# if this script is running outside of gitlab CI
test -z ${SCHED_TYPE+x} && SCHED_TYPE="objectstore"

die() { echo "$@" 1>&2 ; exit 1; }

usage() {
  echo "Script to create a Kubernetes instance and run a system test script."
  echo ""
  echo "Usage: $0 -n <namespace> -s <systemtest_script> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                     Shows help output."
  echo "  -n <namespace>:                 Specify the Kubernetes namespace."
  echo "  -s <systemtest_script>:         Path to the system test script."
  echo "  -p <gitlab pipeline ID>:        GitLab pipeline ID."
  echo "  -i <docker image tag>:          Docker image tag for the deployment."
  echo "  -t <timeout>:                   Timeout for the system test in seconds."
  echo "  -e <eos_configmap>:             Path to the EOS configmap file."
  echo "  -a <additional_k8_resources>:   Path to additional Kubernetes resources."
  echo "  -c <tpsrv count>:               Set the number of tape servers to spawn."
  echo "  -k:                             Keep the namespace after system test script run if successful."
  echo "  -O:                             Use Ceph account associated with this node (wipe content before tests); by default, use local VFS."
  echo "  -D:                             Use Oracle account associated with this node (wipe content before tests); by default, use local SQLite DB."
  echo "  -S:                             Use systemd to manage services inside containers."
  echo "  -U:                             Run database unit test only."
  echo "  -C:                             Cleanup leftover Kubernetes namespaces."
  echo "  -u:                             Prepare the pods to run the Liquibase test."
  echo "  -T:                             Execute tests for external tape formats."
  echo "  -Q:                             Create the cluster using the last ctageneric image from main."
  exit 1
}

# options that must be passed to create_instance
# always delete DB and OBJECTSTORE for tests
CREATE_OPTS="-D -O"

while getopts "n:d:s:p:b:e:a:t:c:ukDOSUCTQ" o; do
    case "${o}" in
        s)
            systemtest_script=${OPTARG}
            test -f ${systemtest_script} || error="${error}systemtest script file ${systemtest_script} does not exist\n"
            ;;
        n)
            namespace=${OPTARG}
            ;;
        d)
            CREATE_OPTS="${CREATE_OPTS} -d ${OPTARG}"
            ;;
        p)
            CREATE_OPTS="${CREATE_OPTS} -p ${OPTARG}"
            ;;
        e)
            config_eos=${OPTARG}
            test -f ${config_eos} || error="${error}EOS configmap file ${config_eos} does not exist\n"
            ;;
        a)
            CREATE_OPTS="${CREATE_OPTS} -a ${OPTARG}"
            ;;
        t)
            SYSTEMTEST_TIMEOUT=${OPTARG}
            ;;
        c)
            tpsrv_count=${OPTARG}
            ;;
        k)
            keepnamespace=1
            ;;
        D)
            useoracle=1
            ;;
        O)
            useceph=1
            ;;
        S)
            usesystemd=1
            ;;
        U)
            CREATE_OPTS="${CREATE_OPTS} -U"
            PREFLIGHTTEST_SCRIPT='/usr/bin/true' # we do not run preflight test in the context of unit tests
            ;;
        C)
            cleanup_namespaces=1
            ;;
        u)
            CREATE_OPTS="${CREATE_OPTS} -u"
            ;;
        T)
            CREATE_OPTS="${CREATE_OPTS} -T"
            ;;
        Q)
            CREATE_OPTS="${CREATE_OPTS} -Q"
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))


if [ -z "${namespace}" ]; then
    echo "a namespace is mandatory" 1>&2
    usage
fi

if [ -z "${systemtest_script}" ]; then
    echo "a systemtest script is mandatory" 1>&2
    usage
fi

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

# ORACLE_SUPPORT is an external variable of the gitlab-ci to use postgres when CTA is compiled without Oracle
if [ $ORACLE_SUPPORT == "OFF" ] ; then
  database_configmap="pgsql-pod-creds.yaml.example"
  CREATE_OPTS="${CREATE_OPTS} -d ${database_configmap}"
  useoracle=0
fi

if [ $useoracle == 1 ] ; then
    database_credentials=$(find /opt/kubernetes/CTA/ | grep oracle-creds.yaml | head -1)
    if [ "-${database_credentials}-" == "--" ]; then
      die "Oracle database requested but not database configuration was found."
    else
      CREATE_OPTS="${CREATE_OPTS} -d ${database_credentials}"
    fi
fi

# SCHED_TYPE is an external variable of the gitlab-ci to use postgres scheduler backend if CTA is compiled with it
if [ $SCHED_TYPE == "pgsched" ] ; then
  schedstore_creds=$(find /opt/kubernetes/CTA/ | grep pgsched-creds.yaml | head -1)
  CREATE_OPTS="${CREATE_OPTS} -o ${schedstore_creds}"
  useceph=0
fi

if [ $useceph == 1 ] ; then
    objectstore_credentials=$(find /opt/kubernetes/CTA/ | grep objectstore-file.yaml | head -1)
    if [ "-${objectstore_credentials}-" == "--" ]; then
      die "Ceph objecstore requested but not objectstore configuration was found."
    else
      CREATE_OPTS="${CREATE_OPTS} -o ${objectstore_credentials}"
    fi
fi

if [ -n "${config_eos}" ]; then
    CREATE_OPTS="${CREATE_OPTS} -e ${config_eos}"
fi

if [ -n "${tpsrv_count}" ]; then
    CREATE_OPTS="${CREATE_OPTS} -c ${tpsrv_count}"
fi

if [ $usesystemd == 1 ] ; then
    CREATE_OPTS="${CREATE_OPTS} -S"
fi

log_dir="${orchestration_dir}/../../pod_logs/${namespace}"
mkdir -p ${log_dir}

if [ $cleanup_namespaces == 1 ]; then
    echo "Cleaning up old namespaces:"
    kubectl get namespace -o json | jq '.items[].metadata | select(.name != "default" and .name != "kube-system") | .name' | grep -E '\-[0-9]+git'
    kubectl get namespace -o json | jq '.items[].metadata | select(.name != "default" and .name != "kube-system") | .name' | grep -E '\-[0-9]+git' | xargs -itoto ./delete_instance.sh -n toto -D
    echo DONE
fi

function execute_log {
  mycmd=$1
  logfile=$2
  timeout=$3
  echo "$(date): Launching ${mycmd}"
  echo "================================================================================"
  eval "(${mycmd} | tee -a ${logfile}) &"
  echo "================================================================================"
  execute_log_pid=$!
  execute_log_rc=''

  for ((i=0;i<${timeout};i++)); do
    sleep 1
    if [ ! -d /proc/${execute_log_pid} ]; then
      wait ${execute_log_pid}
      execute_log_rc=$?
      break
    fi
  done
  echo $i

  if [ "${execute_log_rc}" == "" ]; then
    echo "TIMEOUTING COMMAND, setting exit status to 1"
    kill -9 -${execute_log_pid}
    execute_log_rc=1
  fi

  if [ "${execute_log_rc}" != "0" ]; then
    if [ $keepnamespace == 0 ] ; then
      echo "FAILURE: cleaning up environment"
      cd ${orchestration_dir}
      ./delete_instance.sh -n ${namespace}
    fi
    echo "FAILURE: environment will not be cleaned up"
    exit 1
  fi
}


# create instance timeout after 10 minutes
execute_log "./create_instance.sh -n ${namespace} ${CREATE_OPTS} 2>&1" "${log_dir}/create_instance.log" ${CREATEINSTANCE_TIMEOUT}

# Launch preflighttest and timeout after ${PREFLIGHTTEST_TIMEOUT} seconds
if [ -x ${PREFLIGHTTEST_SCRIPT} ]; then
  cd $(dirname ${PREFLIGHTTEST_SCRIPT})
  echo "Launching preflight test: ${PREFLIGHTTEST_SCRIPT}"
  execute_log "./$(basename ${PREFLIGHTTEST_SCRIPT}) -n ${namespace} 2>&1" "${log_dir}/$(basename ${PREFLIGHTTEST_SCRIPT}).log" ${PREFLIGHTTEST_TIMEOUT}
  cd ${orchestration_dir}
else
  echo "SKIPPING preflight test: ${PREFLIGHTTEST_SCRIPT} not available"
fi

# launch system test and timeout after ${SYSTEMTEST_TIMEOUT} seconds
cd $(dirname ${systemtest_script})
execute_log "./$(basename ${systemtest_script}) -n ${namespace} 2>&1" "${log_dir}/systests.sh.log" ${SYSTEMTEST_TIMEOUT}
cd ${orchestration_dir}

# delete instance?
if [ $keepnamespace == 1 ] ; then
  exit 0
fi
./delete_instance.sh -n ${namespace}
exit $?
