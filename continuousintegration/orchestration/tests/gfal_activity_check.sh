#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


# Archive some files.
dd if=/dev/urandom of=/root/test_act_gfal bs=15K count=1
xrdcp /root/test_act_gfal root://${EOS_MGM_HOST}/${EOS_DIR}/test_valid_instance
xrdcp /root/test_act_gfal root://${EOS_MGM_HOST}/${EOS_DIR}/test_invalid_instance

# Wait for files to be on tape
wait_for_archive ${EOS_MGM_HOST} ${EOS_DIR}/test_valid_instance ${EOS_DIR}/test_invalid_instance

# Query .well-known tape rest api endpoint to get the sitename
site_name=$(curl --insecure https://${EOS_MGM_HOST}:8443/.well-known/wlcg-tape-rest-api 2>/dev/null | jq .sitename)

# Generate metadata string with correct site name
valid_metadata="{${site_name}: {\"activity\": \"CTA-Test-HTTP-CI-TEST-activity-passing\"}}"
echo "Metadata contents: ${valid_metadata}"

# Bring online with valid activity
BEARER_TOKEN=${TOKEN_EOSPOWER1} gfal-bringonline --staging-metadata "${valid_metadata}" "davs://${EOS_MGM_HOST}:8443/${EOS_DIR}/test_valid_instance"

# Evict
BEARER_TOKEN=${TOKEN_EOSPOWER1} gfal-evict https://${EOS_MGM_HOST}:8443/${EOS_DIR}/test_valid_instance

# Bring online for different instance: No activity should be logged in xrdmgm.log or eosreport
invalid_metadata="{\"NotTheSiteName\": {\"activity\": \"CTA-Test-HTTP-CI-TEST-activity-passing\"}}"
echo "Wrong instance metadata: ${invalid_metadata}"

BEARER_TOKEN=${TOKEN_EOSPOWER1} gfal-bringonline --staging-metadata "${invalid_metadata}" "davs://${EOS_MGM_HOST}:8443/${EOS_DIR}/test_invalid_instance"

# Evict the retrieved file
BEARER_TOKEN=${TOKEN_EOSPOWER1} gfal-evict https://${EOS_MGM_HOST}:8443/${EOS_DIR}/test_invalid_instance
