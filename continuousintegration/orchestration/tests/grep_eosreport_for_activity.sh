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



echo "Looking for http staging activity..."
activity_http=$(cat $(find /var/eos/report/$(date +%Y)/$(date +%m)/ | grep '.eosreport') | grep 'event=stage' | grep 'activity=CTA-Test-HTTP-CI-TEST-activity-passing' | wc -l)

if [${activity} -eq 0 ]; then
    echo "Activity for http not found in eosreport"
    exit 1
fi

echo "Staging activity is correctly set."
echo "Test OK"
exit 0
