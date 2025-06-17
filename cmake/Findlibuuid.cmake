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
#     LIBUUID_FOUND
#     LIBUUID_INCLUDE_DIRS
#     LIBUUID_LIBRARIES

find_path(LIBUUID_INCLUDE_DIRS
  uuid.h
  PATHS /usr/include/uuid
  NO_DEFAULT_PATH)

find_library(LIBUUID_LIBRARIES
  NAME uuid
  PATHS /usr/lib64
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libuuid DEFAULT_MSG
  LIBUUID_INCLUDE_DIRS LIBUUID_LIBRARIES)
