#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
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

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

while getopts "n:" o; do
    case "${o}" in
        n)
            NAMESPACE=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${NAMESPACE}" ]; then
    usage
fi

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

echo "Create json meta data input file"
rm /tmp/metaData
touch /tmp/metaData
echo '{"eosPath": "/eos/ctaeos/file3", "diskInstance": "ctaeos", "archiveId": "4294967296", "size": "420", "checksumType": "ADLER32", "checksumValue": "ac94824f"}' >> /tmp/metaData
echo '{"eosPath": "/eos/ctaeos/file4", "diskInstance": "ctaeos", "archiveId": "4294967297", "size": "420", "checksumType": "ADLER32", "checksumValue": "ac94824f"}' >> /tmp/metaData
kubectl cp /tmp/metaData ${NAMESPACE}/ctafrontend:/root/

echo
echo "ENABLE CTAFRONTEND TO EXECUTE CTA ADMIN COMMANDS"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin admin add --username ctafrontend --comment "for restore files test"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin admin add --username ctaeos --comment "for restore files test"

echo
echo "ADD FRONTEND GATEWAY TO EOS"
echo "kubectl -n ${NAMESPACE} exec ctaeos -- bash eos root://${EOSINSTANCE} -r 0 0 vid add gateway ${FRONTEND_IP} grpc"
kubectl -n ${NAMESPACE} exec ctaeos -- eos -r 0 0 vid add gateway ${FRONTEND_IP} grpc

echo
echo "COPY REQUIRED FILES TO FRONTEND POD"
echo "sudo kubectl cp ${NAMESPACE}/ctacli:/etc/cta/cta-cli.conf /etc/cta/cta-cli.conf"
echo "sudo kubectl cp /etc/cta/cta-cli.conf ${NAMESPACE}/ctafrontend:/etc/cta/cta-cli.conf"
sudo kubectl cp ${NAMESPACE}/ctacli:/etc/cta/cta-cli.conf /etc/cta/cta-cli.conf
sudo kubectl cp /etc/cta/cta-cli.conf ${NAMESPACE}/ctafrontend:/etc/cta/cta-cli.conf

echo
kubectl cp ~/CTA-build/cmdline/standalone_cli_tools/eos_namespace_injection/cta-eos-namespace-inject ${NAMESPACE}/ctafrontend:/usr/bin/
echo "kubectl -n ${NAMESPACE} exec ctafrontend -- bash -c XrdSecPROTOCOL=sss XrdSecSSSKT=/etc/cta/eos.sss.keytab cta-eos-namespace-inject --json /root/json.json"
kubectl -n ${NAMESPACE} exec ctafrontend -- bash -c "XrdSecPROTOCOL=sss XrdSecSSSKT=/etc/cta/eos.sss.keytab cta-eos-namespace-inject --json /root/metaData"