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

# NooBaa-CTA Integration Test Suite
# Runs test_client.sh and test_retrieval.sh in CI environment

SECONDS_FILE_NAME=`basename $0`
echo "Launching test ${SECONDS_FILE_NAME}..."

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

while getopts "n:" o; do
    case "${o}" in
        n) NAMESPACE=${OPTARG} ;;
        *) usage ;;
    esac
done

if [ -z "${NAMESPACE}" ]; then usage; fi

# Source common test preparation
. prepare_tests.sh -n ${NAMESPACE}

echo "Starting NooBaa-CTA integration tests in namespace: ${NAMESPACE}"

# Change to NooBaa integration scripts directory
cd ../../../noobaa-cta-integration/scripts/

# Run test_client.sh (S3 Glacier archival workflow)
echo "🚀 Running S3 Glacier archival test..."
./test_client_simple.sh
archival_result=$?

# Run test_retrieval.sh (S3 Glacier retrieval workflow) 
echo "🚀 Running S3 Glacier retrieval test..."
./test_retrieval_simple.sh
retrieval_result=$?

# Report results
echo "=== NOOBAA-CTA INTEGRATION TEST RESULTS ==="
if [ $archival_result -eq 0 ]; then
    echo "✅ S3 Glacier Archival Test: PASSED"
else
    echo "❌ S3 Glacier Archival Test: FAILED"
fi

if [ $retrieval_result -eq 0 ]; then
    echo "✅ S3 Glacier Retrieval Test: PASSED"
else
    echo "❌ S3 Glacier Retrieval Test: FAILED"
fi

# Set CI exit code
if [ $archival_result -eq 0 ] && [ $retrieval_result -eq 0 ]; then
    echo "🎉 ALL NOOBAA-CTA TESTS PASSED"
    exit 0
else
    echo "SOME NOOBAA-CTA TESTS FAILED"
    exit 1
fi
