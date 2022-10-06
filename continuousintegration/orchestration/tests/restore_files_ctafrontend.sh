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

EOSINSTANCE=ctaeos

usage() { cat <<EOF 1>&2
Usage: $0 -f <archive_file_id>
EOF
exit 1
}

while getopts "I:f:i:" o; do
  case "${o}" in
    I)
      ARCHIVE_FILE_ID=${OPTARG}
      ;;
    f)
      TEST_FILE_NAME=${OPTARG}
      ;;
    i)
      EOSINSTANCE=${OPTARG}
      ;;
    *)
      usage
      ;;
  esac    
done
shift $((OPTIND-1))

if [ -z "${ARCHIVE_FILE_ID}" ]; then
  usage
fi

if [ ! -z "${error}" ]; then
  echo -e "ERROR:\n${error}"
  exit 1
fi

echo
echo "RESTORE DELETED FILE"
echo "./cta-restore-deleted-files --id ${ARCHIVE_FILE_ID} --copynb 1 --debug"
chmod +x /usr/bin/cta-restore-deleted-files
./usr/bin/cta-restore-deleted-files --id ${ARCHIVE_FILE_ID} --copynb 1 --debug
