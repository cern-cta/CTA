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

# Create database schema.


# Table client_tests
#   filename
#   archived  - amount of time a file has been archived, for copies.
#   staged - amount of time the evict
#   deleted   - if the file is currently not on tape because it has been deleted.
#   evicted   - amount of times the evict call has been called for the file.
cat <<EOF > /opt/run/bin/tracker.schema

CREATE TABLE client_tests(
       filename TEXT PRIMARY KEY,
       archived INTEGER DEFAULT 0,
       staged   INTEGER DEFAULT 0,
       deleted  INTEGER DEFAULT 0,
       evicted  INTEGER DEFAULT 0
);
EOF

sqlite3 trackerdb < /opt/run/bin/tracker.schema
