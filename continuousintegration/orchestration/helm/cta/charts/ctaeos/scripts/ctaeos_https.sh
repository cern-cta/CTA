#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022-2024 CERN
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

# Move latest mkcert binary to where mkcert-ssl.sh expects it
cp /opt/run/bin/external/mkcert-v*-linux-amd64 /usr/bin/mkcert
chmod +x /usr/bin/mkcert

# run vanilla mkcert-ssl.sh
bash /opt/run/bin/external/mkcert-ssl.sh
# Make sure that CAROOT has proper rights
chown -R daemon:daemon /etc/grid-security/certificates/*

if [ $? -eq 0 ]; then
  echo "Host certificate created"
else
  echo "ERROR: Host certificate creation failed!!!"
fi
