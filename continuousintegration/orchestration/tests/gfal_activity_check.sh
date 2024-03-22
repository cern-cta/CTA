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

# Archive some file.
dd id=/dev/urandom of=/root/test_act bs=15K count=1
xrdcp /root/test_act root://${EOSINSTANCE}/${EOSDIR}/test_act

# Query .well-known tape rest api endpoint to get the sitename
site_name=$(curl --insecure https://ctaeos:8444/.well-known/wlcg-tape-rest-api 2>/dev/null | jq .sitename)

metadata="{${sitename}: {\"activity\": \"CTA-Test-HTTP-CI-TEST-activity-passing\"}}"

echo "Metadata contents: ${metadata}"

# Bring online
BEARER_TOKEN=${TOKEN_EOSPOWER} gfal-bringonline --staging-metadata "${metadata}" "davs://${EOSINSTANCE}:8444/${EOSDIR}/test_act"
