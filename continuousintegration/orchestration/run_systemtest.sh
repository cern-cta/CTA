#!/bin/bash

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
# default systemtest timeout is 1 hour
SYSTEMTEST_TIMEOUT=3600

die() { echo "$@" 1>&2 ; exit 1; }

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> -s <systemtest_script> [-p <gitlab pipeline ID> | -b <build tree base> -B <build tree subdir> ] [-t <systemtest timeout in seconds>] [-k] [-O] [-D] [-S]

Options:
  -b    The directory containing both the source and the build tree for CTA. It will be mounted RO in the
        containers.
  -B    The subdirectory within the -b directory where the build tree is.
  -k    keep namespace after systemtest_script run if successful
  -O    use Ceph account associated to this node (wipe content before tests), by default use local VFS
  -D    use Oracle account associated to this node (wipe content before tests), by default use local sqlite DB
  -S    Use systemd to manage services inside containers 


Create a kubernetes instance and launch the system test script specified.
Makes sure the created instance is cleaned up at the end and return the status of the system test.
EOF

exit 1
}

# options that must be passed to create_instance
# always delete DB and OBJECTSTORE for tests
CREATE_OPTS="-D -O"

while getopts "n:s:p:b:B:t:kDOS" o; do
    case "${o}" in
        s)
            systemtest_script=${OPTARG}
            test -f ${systemtest_script} || error="${error}Objectstore configmap file ${config_objectstore} does not exist\n"
            ;;
        n)
            namespace=${OPTARG}
            ;;
        p)
            CREATE_OPTS="${CREATE_OPTS} -p ${OPTARG}"
            ;;
        b)
            CREATE_OPTS="${CREATE_OPTS} -b ${OPTARG}"
            ;;
        B)
            CREATE_OPTS="${CREATE_OPTS} -B ${OPTARG}"
            ;;
        t)
            SYSTEMTEST_TIMEOUT=${OPTARG}
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

if [ $useoracle == 1 ] ; then
    database_configmap=$(find /opt/kubernetes/CTA/ | grep yaml$ | grep database | head -1)
    if [ "-${database_configmap}-" == "--" ]; then
      die "Oracle database requested but not database configuration was found."
    else
      CREATE_OPTS="${CREATE_OPTS} -d ${database_configmap}"
    fi    
fi

if [ $useceph == 1 ] ; then
    objectstore_configmap=$(find /opt/kubernetes/CTA/ | grep yaml$ | grep objectstore | head -1)
    if [ "-${objectstore_configmap}-" == "--" ]; then
      die "Ceph objecstore requested but not objectstore configuration was found."
    else
      CREATE_OPTS="${CREATE_OPTS} -o ${objectstore_configmap}"                                         
    fi
fi

if [ $usesystemd == 1 ] ; then
    CREATE_OPTS="${CREATE_OPTS} -S"
fi

log_dir="${orchestration_dir}/../../pod_logs/${namespace}"
mkdir -p ${log_dir}


function execute_log {
  mycmd=$1
  logfile=$2
  timeout=$3
  echo "$(date): Launching ${mycmd}"
  eval "(${mycmd} | tee -a ${logfile}) &"
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
    echo "FAILURE: cleaning up environment"
    cd ${orchestration_dir}
    if [ $keepnamespace == 0 ] ; then
      ./delete_instance.sh -n ${namespace}
    fi
    exit 1
  fi
}

# create instance timeout after 10 minutes
execute_log "./create_instance.sh -n ${namespace} ${CREATE_OPTS} 2>&1" "${log_dir}/create_instance.log" ${CREATEINSTANCE_TIMEOUT}

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
