#!/bin/sh

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

EOS_MGM_LOG=/var/log/eos/mgm/xrdlog.mgm

if test -f ${EOS_MGM_LOG}; then
  echo "${EOS_MGM_LOG} exists"
else
  echo "${EOS_MGM_LOG} does not exist or is not a regular file"
  exit 1
fi

echo "Grepping ${EOS_MGM_LOG} for ERROR messages"
if grep -q ERROR ${EOS_MGM_LOG}; then
  echo "Found ERROR messages in ${EOS_MGM_LOG}"
#  exit 1
else
  echo "No ERROR messages found in ${EOS_MGM_LOG}"
  exit 0
fi
