#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022-2024 CERN
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

die() {
  echo "$@" 1>&2
  exit 1;
}

usage() {
  echo "Script to create a Kubernetes instance and run a system test script in this instance."
  echo ""
  echo "Usage: $0 -n <namespace> -s <systemtest_script> -o <scheduler_config> -d <catalogue_config> -i <image_tag> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                     Shows help output."
  echo "  -n, --namespace <namespace>:    Specify the Kubernetes namespace (mandatory)."
  echo "  -s, --test-script <script>:     Path to the system test script (mandatory)."
  echo "  -o, --scheduler-config <file>:  Path to the scheduler config file (mandatory)."
  echo "  -d, --catalogue-config <file>:  Path to the catalogue config file (mandatory)."
  echo "  -i, --image-tag <tag>:          Docker image tag for the deployment (mandatory)."
  echo "  -r, --registry-host <host>:     Provide the Docker registry host. Defaults to \"gitlab-registry.cern.ch/cta\"."
  echo "  -t, --test-timeout <seconds>:   Timeout for the system test in seconds."
  echo "      --spawn-options <options>:  Additional options to pass during pod spawning. These are passed verbatim to the create_instance script."
  echo "      --test-options <options>:   Additional options to pass verbatim to the test script."
  echo "  -K, --keep-namespace:           Keep the namespace after the system test script run if successful."
  echo "  -C, --cleanup-namespaces:       Clean up leftover Kubernetes namespaces."
  exit 1
}

execute_cmd_with_log() {
  mycmd=$1
  logfile=$2
  timeout=$3
  start_time=$(date +%s)
  echo "================================================================================"
  echo "Launching ${mycmd}"
  echo "================================================================================"
  echo "Logging to: ${logfile}"
  {
    timeout "${timeout}" bash -c "${mycmd}"
  } 2>&1  | (tee -a "${logfile}")
  execute_log_rc=$?

  end_time=$(date +%s)
  elapsed_time=$(( end_time - start_time ))
  echo "================================================================================"
  echo "Waiting for process took ${elapsed_time} seconds"

  if [ "${execute_log_rc}" == "" ]; then
    echo "TIMEOUTING COMMAND, setting exit status to 1" 1>&2
    kill -9 -${execute_log_pid}
    execute_log_rc=1
  fi

  if [ "${execute_log_rc}" != "0" ]; then
    echo "Process exited with exit code: ${execute_log_rc}." 1>&2
    if [ $keepnamespace == 0 ] ; then
      echo "Cleaning up environment"
      cd ${orchestration_dir}
      ./delete_instance.sh -n ${namespace}
      echo "Cleanup completed"
    else
      echo "Skipping environment clean up"
    fi
    die "Command execution of $mycmd failed"
  fi
}

run_systemtest() {

  orchestration_dir=${PWD} # orchestration directory so that we can come back here and launch delete_instance during cleanup
  create_instance_timeout=1400 # time out for the create_instance.sh script
  preflighttest_script='tests/preflighttest.sh'
  preflighttest_timeout=60 # default preflight checks timeout is 60 seconds

  # Argument defaults
  keepnamespace=0 # keep or drop namespace after systemtest_script? By default drop it.
  systemtestscript_timeout=3600 # default systemtest timeout is 1 hour
  cleanup_namespaces=0 # by default do not cleanup leftover namespaces
  spawn_options=" --wipe-catalogue --wipe-scheduler"
  extra_test_options=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -s|--test-script)
        systemtest_script="$2"
        test -f ${systemtest_script} || die "ERROR: systemtest script file ${systemtest_script} does not exist\n"
        shift ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -t|--test-timeout)
        systemtestscript_timeout="$2"
        shift ;;
      -r|--registry-host)
        registry_host="$2"
        spawn_options+=" --registry-host ${registry_host}"
        shift ;;
      -i|--image-tag)
        image_tag="$2"
        spawn_options+=" --image-tag ${image_tag}"
        shift ;;
      -o|--scheduler-config)
        scheduler_config="$2"
        test -f "${scheduler_config}" || die "ERROR: Scheduler config file ${scheduler_config} does not exist"
        spawn_options+=" --scheduler-config ${scheduler_config}"
        shift ;;
      -d|--catalogue-config)
        catalogue_config="$2"
        test -f "${catalogue_config}" || die "ERROR: catalogue config file ${catalogue_config} does not exist"
        spawn_options+=" --catalogue-config ${catalogue_config}"
        shift ;;
      --spawn-options)
        extra_spawn_options="$2"
        shift ;;
      --test-options)
        extra_test_options="$2"
        shift ;;
      -K|--keep-namespace) keepnamespace=1 ;;
      -C|--cleanup-namespaces) cleanup_namespaces=1 ;;
      *)
        echo "Unsupported argument: $1"
        usage
        ;;
    esac
    shift
  done

  # Argument checks
  if [ -z "${namespace}" ]; then
    echo "Missing mandatory argument: -n | --namespace"
    usage
  fi
  if [ -z "${systemtest_script}" ]; then
    echo "Missing mandatory argument: -s | --test-script"
    usage
  fi
  if [ -z "${image_tag}" ]; then
    echo "Missing mandatory argument: -i | --image-tag"
    usage
  fi
  if [ -z "${scheduler_config}" ]; then
    echo "Missing mandatory argument: -o | --scheduler-config"
    usage
  fi
  if [ -z "${catalogue_config}" ]; then
    echo "Missing mandatory argument: -d | --catalogue-config"
    usage
  fi

  log_dir="${orchestration_dir}/../../pod_logs/${namespace}"
  mkdir -p ${log_dir}

  if [ $cleanup_namespaces == 1 ]; then
      echo "Cleaning up old namespaces:"
      kubectl get namespace -o json | jq '.items[].metadata | select(.name != "default" and .name != "kube-system") | .name' | grep -E '\-[0-9]+git'
      kubectl get namespace -o json | jq '.items[].metadata | select(.name != "default" and .name != "kube-system") | .name' | grep -E '\-[0-9]+git' | xargs -itoto ./delete_instance.sh -n toto -D
      echo "Cleanup complete"
  fi

  # create instance timeout after 10 minutes
  execute_cmd_with_log "./create_instance.sh -n ${namespace} ${spawn_options} ${extra_spawn_options}" "${log_dir}/create_instance.log" ${create_instance_timeout}

  # Launch preflighttest and timeout after ${preflighttest_timeout} seconds
  if [ -x ${preflighttest_script} ]; then
    cd $(dirname ${preflighttest_script})
    echo "Launching preflight test: ${preflighttest_script}"
    execute_cmd_with_log "./$(basename ${preflighttest_script}) -n ${namespace}" "${log_dir}/$(basename ${preflighttest_script}).log" ${preflighttest_timeout}
    cd ${orchestration_dir}
  else
    echo "Skipping preflight test: ${preflighttest_script} not available"
  fi

  # launch system test and timeout after ${systemtestscript_timeout} seconds
  cd $(dirname ${systemtest_script})
  execute_cmd_with_log "./$(basename ${systemtest_script}) -n ${namespace} ${extra_test_options}" "${log_dir}/test.log" ${systemtestscript_timeout}
  cd ${orchestration_dir}

  # delete instance?
  if [ $keepnamespace == 1 ] ; then
    exit 0
  fi
  ./delete_instance.sh -n ${namespace}
  exit $?
}

run_systemtest "$@"
