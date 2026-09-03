#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Derived from https://gitlab.cern.ch/ci-tools/ci-web-deployer
# Merges Dockerfile and deploy-eos.sh
# From commit 15c6bdccbee313df5601ce8df34fc4455fe92905
#
# Copies provided artifacts and updates the corresponding RPM repository metadata
# Based on the script by Borja Aparicio April 2016

set -Eeuo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

usage() {
  echo
  echo "Usage: $0 [arguments]"
  echo
  echo "Credentials:"
  echo "  --eos-username     <username>    :    Account username for EOS."
  echo "  EOS_ACCOUNT_PASSWORD must contain the account password."
  echo
  echo "Directory selection:"
  echo "  --local-source-dir <dir>         :    Local directory that will be uploaded to the provided --eos-target-dir."
  echo "  --eos-source-dir   <dir>         :    EOS directory that will be copied to the provided --eos-target-dir. Must be used with --cta-version."
  echo "  --eos-target-dir   <dir>         :    EOS directory where to upload the files to."
  echo "  --cta-version      <cta_version> :    CTA release tag, with or without its leading v."
  echo
  exit 1
}

upload_to_eos() {

  local eos_account_username=""
  local eos_target_dir=""
  local local_source_dir=""
  local eos_source_dir=""
  local cta_version=""
  local repository_dir=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --eos-username)
        if [[ $# -gt 1 ]]; then
          eos_account_username="$2"
          shift
        else
          log_error "Error: --eos-username requires an argument"
          usage
        fi
        ;;
      --eos-target-dir)
        if [[ $# -gt 1 ]]; then
          eos_target_dir="$2"
          shift
        else
          log_error "Error: --eos-target-dir requires an argument"
          usage
        fi
        ;;
      --local-source-dir)
        if [[ $# -gt 1 ]]; then
          local_source_dir="$2"
          shift
        else
          log_error "Error: --local-source-dir requires an argument"
          usage
        fi
        ;;
      --eos-source-dir)
        if [[ $# -gt 1 ]]; then
          eos_source_dir="$2"
          shift
        else
          log_error "Error: --eos-source-dir requires an argument"
          usage
        fi
        ;;
      --cta-version)
        if [[ $# -gt 1 ]]; then
          cta_version="$2"
          shift
        else
          log_error "Error: --cta-version requires an argument"
          usage
        fi
        ;;
      *)
        echo "Invalid argument: $1"
        usage
        ;;
    esac
    shift
  done

  if [[ -z "${eos_account_username}" ]]; then
    echo "Failure: Missing mandatory argument --eos-username"
    usage
  fi

  if [[ -z "${EOS_ACCOUNT_PASSWORD:-}" ]]; then
    log_error "Failure: EOS_ACCOUNT_PASSWORD is not set"
    exit 1
  fi

  if [[ -z "${eos_target_dir}" ]]; then
    echo "Failure: Missing mandatory argument --eos-target-dir"
    usage
  fi

  if [[ -z "${local_source_dir}" ]] && [[ -z "${eos_source_dir}" ]]; then
    echo "Failure: Missing mandatory argument --local-source-dir or --eos-source-dir"
    usage
  fi

  if [[ -n "${local_source_dir}" ]] && [[ -n "${eos_source_dir}" ]]; then
    echo "Failure: Do not use both arguments --local-source-dir and --eos-source-dir"
    usage
  fi

  # Check the source directory exists
  if [[ -n "${local_source_dir}" ]] && [[ ! -d "${local_source_dir}" ]]; then
    log_error "ERROR: Source directory ${local_source_dir} doesn't exist"
    exit 1
  fi

  # Check the cta_version argument was received
  if [[ -n "${eos_source_dir}" ]] && [[ -z "${cta_version}" ]]; then
    log_error "ERROR: Argument --eos-source-dir should be used with --cta-version"
    exit 1
  fi

  if [[ -n "${cta_version}" ]]; then
    cta_version=${cta_version#v}
  fi

  # The target corresponds to the source root, with RPMs stored in its architecture subdirectory
  repository_dir="${eos_target_dir}/x86_64"

  if [[ "${repository_dir}" != "/eos/user/c/ctareg/www/cta-repo/RPMS/x86_64" ]] && \
     [[ ! "${repository_dir}" =~ ^/eos/user/c/ctareg/www/public/cta-public-repo/(unstable|testing|stable)/cta-[1-9][0-9]*/[A-Za-z0-9._-]+/cta/x86_64$ ]]; then
    log_error "ERROR: Refusing to publish outside an approved repository: ${repository_dir}"
    exit 1
  fi

  # Keep this job's credentials separate from any cache provided by the runner and always destroy them
  export KRB5CCNAME="FILE:$(mktemp)"
  trap 'kdestroy 2>/dev/null || true; rm -f -- "${KRB5CCNAME#FILE:}"' EXIT

  if ! printf '%s\n' "${EOS_ACCOUNT_PASSWORD}" | kinit "${eos_account_username}@CERN.CH" >/dev/null 2>&1; then
    log_error "ERROR: Failed to get Krb5 credentials for $eos_account_username"
    exit 1
  fi

  if [[ -n "${local_source_dir}" ]]; then
    # Rely on xrootd to do the copy of files to EOS
    if ! xrdcp --force --recursive "${local_source_dir}"/ "root://eoshome.cern.ch/${eos_target_dir}/" >/dev/null 2>&1; then
      log_error "ERROR: Failed to copy files to ${eos_target_dir} via xrdcp"
      exit 1
    fi
  fi

  if [[ -n "${eos_source_dir}" ]]; then
    # Rely on xrootd to copy the files, inside EOS, with the provided cta-version
    # ls -R handles recursion; each matching path passed to xrdcp is an individual RPM file
    if ! xrdfs root://eoshome.cern.ch/ ls -R "${eos_source_dir}" \
      | grep -F -- "${cta_version}." \
      | while IFS= read -r rpm_path; do
          relative_path=${rpm_path#"${eos_source_dir}"/}
          xrdcp --force \
            "root://eoshome.cern.ch/${rpm_path}" \
            "root://eoshome.cern.ch/${eos_target_dir}/${relative_path}" \
            >/dev/null 2>&1
        done; then
      log_error "ERROR: Failed to copy release ${cta_version} files from ${eos_source_dir} to ${eos_target_dir} via xrdcp"
      exit 1
    fi
  fi

  if ! ssh \
      -o StrictHostKeyChecking=no \
      -o GSSAPIAuthentication=yes \
      -o GSSAPITrustDNS=yes \
      -o GSSAPIDelegateCredentials=yes \
      "${eos_account_username}@lxplus.cern.ch" \
      /usr/bin/createrepo_c --update -- "${repository_dir}"; then
    log_error "ERROR: Failed to update repository metadata in ${repository_dir}"
    exit 1
  fi

  echo "Repository metadata updated successfully"
}

upload_to_eos "$@"
