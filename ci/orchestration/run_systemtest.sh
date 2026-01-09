#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# makes sure that set -e from gitlab CI scripts is not enabled
set +e
# enable pipefail to get the return code of a failed command in pipeline and not the return code of the last command in pipeline:
# http://stackoverflow.com/questions/6871859/piping-command-output-to-tee-but-also-save-exit-code-of-command
set -o pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_wrapper.sh"

usage() {
  echo
  echo "Script to create a Kubernetes instance and run a system test script in this instance."
  echo
  echo "Usage: $0 -n <namespace> -s <systemtest_script> -o <scheduler_config> -d <catalogue_config> -i <image_tag> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "  -s, --test-script <script>:         Path to the system test script."
  echo "  -o, --scheduler-config <file>:      Path to the scheduler config file."
  echo "  -d, --catalogue-config <file>:      Path to the catalogue config file."
  echo "  -r, --cta-image-repository <repo>:  The CTA Docker image name. Defaults to \"gitlab-registry.cern.ch/cta/ctageneric\"."
  echo "  -i, --cta-image-tag <tag>:          The CTA Docker image tag."
  echo "      --eos-image-repository <repo>:  The EOS Docker image name. Defaults to \"gitlab-registry.cern.ch/cta/ctageneric\"."
  echo "      --eos-image-tag <tag>:          The EOS Docker image tag."
  echo "  -t, --test-timeout <seconds>:       Timeout for the system test in seconds."
  echo "      --spawn-options <options>:      Additional options to pass during pod spawning. These are passed verbatim to the create_instance script."
  echo "      --test-options <options>:       Additional options to pass verbatim to the test script."
  echo "  -K, --keep-namespace:               Keep the namespace after the system test script run if successful."
  echo "  -C, --cleanup-namespaces:           Clean up leftover Kubernetes namespaces."
  echo "      --skip-preflight:               Skips the preflight tests."
  echo
  exit 1
}

execute_cmd_with_log() {
  mycmd=$1
  logfile=$(realpath "$2")
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

  if [[ "${execute_log_rc}" == "" ]]; then
    echo "TIMEOUTING COMMAND, setting exit status to 1" 1>&2
    kill -9 -${execute_log_pid}
    execute_log_rc=1
  fi

  if [[ "${execute_log_rc}" != "0" ]]; then
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
  create_instance_timeout=600 # time out for the create_instance.sh script
  preflight_checks_script="tests/preflight_checks.sh"
  preflight_checks_timeout=60 # default preflight checks timeout is 60 seconds
  postrun_checks_script="tests/postrun_checks.sh"
  postrun_checks_timeout=30 # default preflight checks timeout is 60 seconds

  # Argument defaults
  keepnamespace=0 # keep or drop namespace after systemtest_script? By default drop it.
  systemtestscript_timeout=3600 # default systemtest timeout is 1 hour
  cleanup_namespaces=0 # by default do not cleanup leftover namespaces
  spawn_options=" --reset-catalogue --reset-scheduler"
  extra_test_options=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -s|--test-script)
        systemtest_script="$2"
        test -f ${systemtest_script} || die "ERROR: systemtest script file ${systemtest_script} does not exist\n"
        shift ;;
      --skip-preflight)
        preflight_checks_script="" ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -t|--test-timeout)
        systemtestscript_timeout="$2"
        shift ;;
      -r|--cta-image-repository)
        spawn_options+=" --cta-image-repository $2"
        shift ;;
      -i|--cta-image-tag)
        cta_image_tag="$2"
        spawn_options+=" --cta-image-tag ${cta_image_tag}"
        shift ;;
      --eos-image-repository)
        spawn_options+=" --eos-image-repository $2"
        shift ;;
      --eos-image-tag)
        spawn_options+=" --eos-image-tag $2"
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
  if [[ -z "${namespace}" ]]; then
    die_usage "Missing mandatory argument: -n | --namespace"
  fi
  if [[ -z "${systemtest_script}" ]]; then
    die_usage "Missing mandatory argument: -s | --test-script"
  fi
  if [[ -z "${cta_image_tag}" ]]; then
    die_usage "Missing mandatory argument: -i | --cta-image-tag"
  fi
  if [[ -z "${scheduler_config}" ]]; then
    die_usage "Missing mandatory argument: -o | --scheduler-config"
  fi
  if [[ -z "${catalogue_config}" ]]; then
    die_usage "Missing mandatory argument: -d | --catalogue-config"
  fi

  log_dir="${orchestration_dir}/../../pod_logs/${namespace}"
  mkdir -p "${log_dir}"
  if [[ -d "${log_dir}" ]]; then
    # Delete log contents of previous runs if they exist
    rm -rf "${log_dir:?}/"*
  fi

  old_namespaces=$(kubectl get namespace -o json | jq -r '.items[].metadata.name | select(. != "default" and (test("^kube-") | not))')
  if [[ $cleanup_namespaces == 1 ]]; then
    echo "Cleaning up old namespaces"
    echo $old_namespaces
    echo $old_namespaces | xargs -itoto ./delete_instance.sh -n toto -D
    echo "Cleanup complete"
  elif kubectl get namespace ${namespace} > /dev/null 2>&1; then
    die "Namespace ${namespace} already exists"
  elif [[ -n "$old_namespaces" ]]; then
    error_usage "Other namespaces exist: $old_namespaces"
  fi


  # create instance timeout after 10 minutes
  execute_cmd_with_log "./create_instance.sh -n ${namespace} ${spawn_options} ${extra_spawn_options}" "${log_dir}/create_instance.log" ${create_instance_timeout}

  # Launch preflight_checks and timeout after ${preflight_checks_timeout} seconds
  if [[ -n "$preflight_checks_script" ]] && [[ -x "$preflight_checks_script" ]]; then
    cd $(dirname "${preflight_checks_script}")
    echo "Launching preflight checks: ${preflight_checks_script}"
    execute_cmd_with_log "./$(basename ${preflight_checks_script}) -n ${namespace}" \
                         "${log_dir}/$(basename ${preflight_checks_script} | cut -d. -f1).log" \
                         ${preflight_checks_timeout}
    cd "${orchestration_dir}"
  else
    echo "Skipping preflight checks: ${preflight_checks_script} not available"
  fi

  # launch system test and timeout after ${systemtestscript_timeout} seconds
  cd $(dirname "${systemtest_script}")
  systemtest_script_basename=$(basename -- "$systemtest_script")
  execute_cmd_with_log "./${systemtest_script_basename} -n ${namespace} ${extra_test_options}" \
                       "${log_dir}/${systemtest_script_basename}.log" \
                       ${systemtestscript_timeout}
  cd "${orchestration_dir}"

  # Launch any final checks
  white_list_file="error_whitelists/${systemtest_script_basename%.*}.txt"
  cd $(dirname "${postrun_checks_script}")
  echo "Launching postrun checks: ${postrun_checks_script}"
  execute_cmd_with_log "./$(basename ${postrun_checks_script}) -n ${namespace} -w ${white_list_file}" \
                        "${log_dir}/$(basename ${postrun_checks_script} | cut -d. -f1).log" \
                        ${postrun_checks_timeout}
  cd "${orchestration_dir}"

  # delete instance?
  if [ $keepnamespace == 1 ] ; then
    exit 0
  fi
  ./delete_instance.sh -n ${namespace}
  exit $?
}

run_systemtest "$@"
