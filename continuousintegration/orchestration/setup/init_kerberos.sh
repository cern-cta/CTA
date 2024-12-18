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
  echo "Initialises kerberos for the ctacli and client pods in the provided namespace."
  echo ""
  echo "Usage: $0 [options] -n <namespace>"
  echo ""
  echo "Options:"
  echo "  -h, --help:                   Show help output."
  echo "  -n|--namespace:               The kubernetes namespaces to execute this in."
  echo "  -r|--krb5-realm <realm>:      The kerberos realm to use. Defaults to TEST.CTA"
  exit 1
}

krb5_realm="TEST.CTA"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -h | --help) usage ;;
    -n|--namespace)
      namespace="$2"
      shift ;;
    -r|--krb5-realm)
      krb5_realm="$2"
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

# Set up kerberos
echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace ${namespace} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
echo "Using kinit for ctacli and client"
kubectl --namespace ${namespace} exec ctacli -- kinit -kt /root/ctaadmin1.keytab ctaadmin1@${krb5_realm}
kubectl --namespace ${namespace} exec client -- kinit -kt /root/user1.keytab user1@${krb5_realm}

echo "klist for client:"
kubectl --namespace ${namespace} exec client -- klist
echo "klist for ctacli:"
kubectl --namespace ${namespace} exec ctacli -- klist
