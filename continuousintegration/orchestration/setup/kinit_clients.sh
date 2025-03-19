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
  echo "Initialises kerberos for the cta-cli and client pods in the provided namespace."
  echo ""
  echo "Usage: $0 [options] -n <namespace>"
  echo ""
  echo "Options:"
  echo "  -h, --help:                   Show help output."
  echo "  -n|--namespace:               The kubernetes namespaces to execute this in."
  echo "  -r|--krb5-realm <realm>:      The kerberos realm to use. Defaults to TEST.CTA"
  exit 1
}

KRB5_REALM="TEST.CTA"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -h | --help) usage ;;
    -n|--namespace)
      NAMESPACE="$2"
      shift ;;
    -r|--krb5-realm)
      KRB5_REALM="$2"
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
  esac
  shift
done

if [ -z "$NAMESPACE" ]; then
  echo "Missing mandatory argument: -n | --namespace" 1>&2
  exit 1
fi

CLIENT_POD="cta-client-0"
CTA_CLI_POD="cta-cli-0"

# Set up kerberos
echo "Running kinit for ${CTA_CLI_POD} and ${CLIENT_POD}"
kubectl --namespace "${NAMESPACE}" exec ${CTA_CLI_POD} -c cta-cli -- kinit -kt /root/ctaadmin1.keytab ctaadmin1@${KRB5_REALM}
kubectl --namespace "${NAMESPACE}" exec ${CTA_CLI_POD} -c cta-cli -- klist
kubectl --namespace "${NAMESPACE}" exec ${CLIENT_POD} -c client -- kinit -kt /root/user1.keytab user1@${KRB5_REALM}
kubectl --namespace "${NAMESPACE}" exec ${CLIENT_POD} -c client -- klist
