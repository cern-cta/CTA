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


usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> -s <systemtest_script> [-p <gitlab pipeline ID>] [-k]

Options:
  -k    keep namespace after systemtest_script run if successful

Create a kubernetes instance and launch the system test script specified.
Makes sure the created instance is cleaned up at the end and return the status of the system test.
EOF

exit 1
}

# options that must be passed to create_instance
# always delete DB and OBJECTSTORE for tests
CREATE_OPTS="-D -O"

while getopts "n:s:p:k" o; do
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
        k)
            keepnamespace=1
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


function execute_log {
  mycmd=$1
  logfile=$2
  echo "$(date): Launching ${mycmd}"
  eval "${mycmd} | tee -a ${logfile}"
  execute_log_rc=$?

  if [ "${execute_log_rc}" != "0" ]; then
    echo "FAILURE: cleaning up environment"
    cd ${orchestration_dir}
    ./delete_instance.sh -n ${namespace}
    exit 1
  fi
}

# create instance
execute_log "./create_instance.sh -n ${namespace} ${CREATE_OPTS} 2>&1" "${orchestration_dir}/../../create_instance.log"

# launch system test
cd $(dirname ${systemtest_script})
execute_log "./$(basename ${systemtest_script}) -n ${namespace} 2>&1" "${orchestration_dir}/../../systests.sh.log"
cd ${orchestration_dir}

# delete instance?
if [ $keepnamespace == 1 ] ; then
  exit 0
fi
./delete_instance.sh -n ${namespace}
exit $?
