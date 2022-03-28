# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
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
# This module will set the following variables:
#     POSTGRES_FOUND
#     POSTGRES_INCLUDE_DIRS
#     POSTGRES_LIBRARIES

find_path(POSTGRES_INCLUDE_DIRS
  libpq-fe.h
  PATHS /usr/include /usr/pgsql-12/include
  NO_DEFAULT_PATH)

find_library(POSTGRES_LIBRARIES
  NAME pq
  PATHS /usr/lib64/ /usr/pgsql-12/lib/
  NO_DEFAULT_PATH)

message (STATUS "POSTGRES_INCLUDE_DIRS = ${POSTGRES_INCLUDE_DIRS}")
message (STATUS "POSTGRES_LIBRARIES    = ${POSTGRES_LIBRARIES}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(postgres DEFAULT_MSG
  POSTGRES_INCLUDE_DIRS POSTGRES_LIBRARIES)
