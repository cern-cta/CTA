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
#     JSON-C_FOUND
#     JSON-C_INCLUDE_DIRS
#     JSON-C_LIBRARIES

find_path(JSON-C_INCLUDE_DIRS
  json.h
  PATHS /usr/include/json-c
  NO_DEFAULT_PATH)

find_library(JSON-C_LIBRARIES
  NAME json-c
  PATHS /usr/lib64
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(json-c DEFAULT_MSG
  JSON-C_INCLUDE_DIRS JSON-C_LIBRARIES)
