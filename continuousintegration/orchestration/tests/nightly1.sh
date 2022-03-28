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

cd /root
FAIL=0

. client_helper.sh
admin_kdestroy
admin_kinit

for DRIVE in $(admin_cta dr ls | grep Down | sed -e 's/ \+/ /g' | cut -d\  -f2); do
  admin_cta dr up ${DRIVE}
done

for ((i=0;i<4;i++)); do
  echo "Launching bash ./client_ar.sh -n 1000 -s 10 -p 4 -v"
  bash ./client_ar.sh -n 1000 -s 10 -p 4 -v &
done

for job in `jobs -p`
do
echo $job
    wait $job || let "FAIL+=1"
done

if [ "$FAIL" == "0" ];
then
echo "YAY!"
else
echo "FAIL! ($FAIL)"
fi

exit $FAIL
