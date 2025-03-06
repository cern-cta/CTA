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

# Derived from https://gitlab.cern.ch/ci-tools/ci-web-deployer
# Merges Dockerfile and deploy-eos.sh
# From commit 15c6bdccbee313df5601ce8df34fc4455fe92905
#
# Copies provided artifacts and launch an additional hook
# Based on the script by Borja Aparicio April 2016

set -e

usage() {
  echo "Usage: $0 [arguments]"
  echo ""
  echo "Credentials:"
  echo "  --eos-username     <username>    :    Account username for EOS."
  echo "  --eos-password     <password>    :    Account password for EOS."
  echo ""
  echo "Directory selection:"
  echo "  --local-source-dir <dir>         :    Local directory that will be uploaded to the provided --eos-target-dir."
  echo "  --eos-source-dir   <dir>         :    EOS directory that will be copied to the provided --eos-target-dir. Must be used with --cta-version."
  echo "  --eos-target-dir   <dir>         :    EOS directory where to upload the files to."
  echo "  --cta-version      <cta_version> :    CTA release version."
  echo ""
  echo "Other:"
  echo "  --hook             <hook>        :    Hook to run on lxplus."
  exit 1
}

upload_to_eos() {

  local eos_account_username=""
  local eos_account_password=""
  local eos_target_dir=""
  local local_source_dir=""
  local eos_source_dir=""
  local cta_version=""
  local hook=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --eos-username)
        if [[ $# -gt 1 ]]; then
          eos_account_username="$2"
          shift
        else
          echo "Error: --eos-username requires an argument"
          usage
        fi
        ;;
      --eos-password)
        if [[ $# -gt 1 ]]; then
          eos_account_password="$2"
          shift
        else
          echo "Error: --eos-password requires an argument"
          usage
        fi
        ;;
      --eos-target-dir)
        if [[ $# -gt 1 ]]; then
          eos_target_dir="$2"
          shift
        else
          echo "Error: --eos-target-dir requires an argument"
          usage
        fi
        ;;
      --hook)
        if [[ $# -gt 1 ]]; then
          hook="$2"
          shift
        else
          echo "Error: --hook requires an argument"
          usage
        fi
        ;;
      --local-source-dir)
        if [[ $# -gt 1 ]]; then
          local_source_dir="$2"
          shift
        else
          echo "Error: --local-source-dir requires an argument"
          usage
        fi
        ;;
      --eos-source-dir)
        if [[ $# -gt 1 ]]; then
          eos_source_dir="$2"
          shift
        else
          echo "Error: --eos-source-dir requires an argument"
          usage
        fi
        ;;
      --cta-version)
        if [[ $# -gt 1 ]]; then
          cta_version="$2"
          shift
        else
          echo "Error: --cta-version requires an argument"
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

  if [ -z "${eos_account_username}" ]; then
    echo "Failure: Missing mandatory argument --eos-username"
    usage
  fi

  if [ -z "${eos_account_password}" ]; then
    echo "Failure: Missing mandatory argument --eos-password"
    usage
  fi

  if [ -z "${eos_target_dir}" ]; then
    echo "Failure: Missing mandatory argument --eos-target-dir"
    usage
  fi

  if [ -z "${local_source_dir}" ] && [ -z "${eos_source_dir}" ]; then
    echo "Failure: Missing mandatory argument --local-source-dir or --eos-source-dir"
    usage
  fi

  if [ -n "${local_source_dir}" ] && [ -n "${eos_source_dir}" ]; then
    echo "Failure: Do not use both arguments --local-source-dir and --eos-source-dir"
    usage
  fi

  # Check the source directory exists
  if [ -n "${local_source_dir}" ] && [ ! -d "${local_source_dir}" ]; then
    echo "ERROR: Source directory ${local_source_dir} doesn't exist"
    exit 1
  fi

  # Check the cta_version argument was received
  if [ -n "${eos_source_dir}" ] && [ -z "${cta_version}" ]; then
    echo "ERROR: Argument --eos-source-dir should be used with --cta-version"
    exit 1
  fi

  # Get credentials
  echo "$eos_account_password" | kinit $eos_account_username@CERN.CH 2>&1 >/dev/null
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to get Krb5 credentials for $eos_account_username"
    exit 1
  fi

  if [ -n "${local_source_dir}" ]; then
    # Rely on xrootd to do the copy of files to EOS
    xrdcp --force --recursive "${local_source_dir}"/ root://eoshome.cern.ch/"${eos_target_dir}"/ 2>&1 >/dev/null
    if [ $? -ne 0 ]; then
      echo "ERROR: Failed to copy files to ${eos_target_dir} via xrdcp"
      exit 1
    fi
  fi

  if [ -n "${eos_source_dir}" ]; then
    # Rely on xrootd to copy the files, inside EOS, with the provided cta-version
    xrdfs root://eoshome.cern.ch/ ls -R "${eos_source_dir}" \
      | grep "${cta_version}" \
      | sed "s|^${eos_source_dir}||" \
      | xargs -I {} xrdcp --force --recursive root://eoshome.cern.ch/"${eos_source_dir}"/{} root://eoshome.cern.ch/"${eos_target_dir}"/{} 2>&1 >/dev/null
    if [ $? -ne 0 ]; then
      echo "ERROR: Failed to copy release ${cta_version} files from ${eos_source_dir} to ${eos_target_dir} via xrdcp"
      exit 1
    fi
  fi

  # Run the provided hook on lxplus
  if [ -n "${hook}" ]; then
    ssh -o StrictHostKeyChecking=no \
        -o GSSAPIAuthentication=yes \
        -o GSSAPITrustDNS=yes \
        -o GSSAPIDelegateCredentials=yes \
        $eos_account_username@lxplus.cern.ch $hook 2>&1
    if [ $? -ne 0 ]; then
      echo "ERROR: Something went wrong while running hook $hook on lxplus"
      exit 1
    fi
    echo "Hook executed successfully"
  fi

  # Destroy credentials
  kdestroy
  if [ $? -ne 0 ]; then
    echo "WARNING: Krb5 credentials for $eos_account_username have not been cleared up"
  fi
}

upload_to_eos "$@"
