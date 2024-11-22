#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

set -e

usage() {
  echo "Sets the required CTA workflows in the ctaeos pod in the provided namespace."
  echo ""
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  -h, --help:         Show help output."
  echo "  -n|--namespace:     The kubernetes namespaces to execute this in."
  exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -h | --help) usage ;;
    -n|--namespace)
      namespace="$2"
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
  esac
  shift
done

if [ -z "$namespace" ]; then
  echo "Missing mandatory argument: -n | --namespace" 1>&2
  exit 1
fi

# Set the workflow rules for archiving, creating tape file replicas in the EOS namespace, retrieving
# files from tape and deleting files.
eos_instance=ctaeos
echo "Setting workflows in namespace ${namespace} pod ${eos_instance}:"
cta_workflow_dir=/eos/${eos_instance}/proc/cta/workflow
for workflow in sync::create.default sync::closew.default sync::archived.default sync::archive_failed.default sync::prepare.default sync::abort_prepare.default sync::evict_prepare.default sync::closew.retrieve_written sync::retrieve_failed.default sync::delete.default; do
  echo "eos attr set sys.workflow.${workflow}=\"proto\" ${cta_workflow_dir}"
  kubectl --namespace ${namespace} exec ${eos_instance} -- bash -c "eos attr set sys.workflow.${workflow}=\"proto\" ${cta_workflow_dir}"
done
