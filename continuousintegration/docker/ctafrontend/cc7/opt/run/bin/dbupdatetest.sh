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

if [[ $CTA_VERSION ]]
then
  echo "CTA_VERSION"
  /entrypoint.sh -d -v ${CTA_VERSION} -f ${CATALOGUE_SOURCE_VERSION} -t ${CATALOGUE_DESTINATION_VERSION} -c update;
else
  echo "COMMIT_ID"
  /entrypoint.sh -d -i ${COMMIT_ID} -f ${CATALOGUE_SOURCE_VERSION} -t ${CATALOGUE_DESTINATION_VERSION} -c update;
fi

