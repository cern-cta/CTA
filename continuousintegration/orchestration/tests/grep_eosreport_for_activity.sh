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



echo "Looking for HTTP staging activity..."
activity_http=$(cat $(find /var/eos/report/$(date +%Y)/$(date +%m)/ | grep '.eosreport') | grep 'event=stage' | grep 'activity=CTA-Test-HTTP-CI-TEST-activity-passing')

report_file_name="/var/eos/report/$(date +%Y)/$(date +%m)/$(date +%Y)$(date +%m)$(date +%d).eosreport"

# Check that activity is set for staging of file `test_valid_instance`
valid_act_stage_logline=$(cat $report_file_name | grep 'event=stage' | grep 'test_valid_instance')
echo "  Stage for valid activity eosrepot log line:"
echo "  ${valid_act_stage_logline}"

if [[ "${valid_act_stage_logline}" =~ .*"&activity=CTA-Test-HTTP-CI-TEST-activity-passing&".* ]]; then
  echo "    Activity was correctly set."
else
  echo "    ERROR: Activity not set for test_valid_instance!"
  exit 1
fi

# Check that activity is NOT set for staging of file `test_invalid_instance`
invalid_act_stage_logline=$(cat $report_file_name | grep 'event=stage' | grep 'test_invalid_instance')
echo "  Invalid activity eosreport log line:"
echo "  ${invalid_act_stage_logline}"

if [[ "${invalid_act_stage_logline}" =~ .*"&activity=&".* ]]; then
  echo "    Activity was not set, as expected"
else
  echo "    ERROR: Activity was set for test_invalid_instance!"
  exit 1
fi

# Check activity is set for XRootD staging request through xrdfs
echo "Looking for XRootD staging activity..."
act_xrd_logline=$(cat $report_file_name | grep 'event=stage' | grep 'XRootD_Act')
echo "  Stage eosreport log line for xrdfs command:"
echo "  ${act_xrd_logline}"

if [[ "${act_xrd_logline}" =~ .*"&activity=XRootD_Act&".* ]]; then
  echo "    Activity was correctly set"
else
  echo "    ERROR: Activity is not set for xrdfs prepare -s"
fi

echo "Activity test: OK"
exit 0
