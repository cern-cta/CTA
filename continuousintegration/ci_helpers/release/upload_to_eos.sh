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
  echo "Usage: $0 [options] --eos-username <username> --eos-password <password> --eos-path <path> --source-dir <dir>"
  echo ""
  echo "Uploads to $EOS_PATH in the EOS namespace the files found in CI_WEBSITE_DIR."
  echo "  --eos-username  <username>:             Account username for EOS."
  echo "  --eos-password  <password>:             Account password for EOS."
  echo "  --eos-path      <path>:                 Path on EOS where to upload the files to."
  echo "  --source-dir    <dir>:                  Directory that will be uploaded to the provided eos-path."
  echo ""
  echo "options:"
  echo "  --hook          <hook>:                 Hook to run on lxplus."
  exit 1
}

upload_to_eos() {

  local eos_account_username=""
  local eos_account_password=""
  local eos_path=""
  local source_dir=""
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
      --eos-path)
        if [[ $# -gt 1 ]]; then
          eos_path="$2"
          shift
        else
          echo "Error: --eos-path requires an argument"
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
      --source-dir)
        if [[ $# -gt 1 ]]; then
          source_dir="$2"
          shift
        else
          echo "Error: --source-dir requires an argument"
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

  if [ -z "${eos_path}" ]; then
    echo "Failure: Missing mandatory argument --eos-path"
    usage
  fi

  if [ -z "${source_dir}" ]; then
    echo "Failure: Missing mandatory argument --source-dir"
    usage
  fi

  # Check the source directory exists
  if [ ! -d $source_dir ]; then
    echo "ERROR: Source directory $source_dir doesn't exist"
    exit 1
  fi

  yum install -y krb5-workstation rsync openssh-clients xrootd-client

  # Get credentials
  echo "$eos_account_password" | kinit $eos_account_username@CERN.CH 2>&1 >/dev/null
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to get Krb5 credentials for $eos_account_username"
    exit 1
  fi

  # Rely in xrootd to do the copy of files to EOS
  xrdcp --force --recursive $source_dir/ root://eoshome.cern.ch/$eos_path/ 2>&1 >/dev/null
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy files to $eos_path via xrdcp"
    exit 1
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
