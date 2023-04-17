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


if [[ $DONOTARCHIVE == 0 ]]; then
    . /root/client_archive.sh
fi

if [[ $ARCHIVEONLY == 1 ]]; then
  echo "Archiveonly mode: exiting"
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 0
fi

. /root/client-gfal2_retrieve.sh

. /root/client-gfal2_evict.sh

if [[ $REMOVE == 1 ]]; then
  . /root/client-gfal2_delete.sh
fi

. /root/client_results.sh
