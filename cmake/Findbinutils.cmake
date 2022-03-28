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
#     BINUTILS_FOUND
#     BINUTILS_INCLUDE_DIRS
#     BINUTILS_LIBRARIES

find_path(BINUTILS_INCLUDE_DIRS
  bfd.h
  PATHS /usr/include
  NO_DEFAULT_PATH)

find_library(BINUTILS_LIBRARIES
  NAME bfd
  PATHS /usr/lib64
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(binutils DEFAULT_MSG
  BINUTILS_INCLUDE_DIRS BINUTILS_LIBRARIES)
