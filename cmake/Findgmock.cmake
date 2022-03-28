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
#     GMOCK_SRC

set(GMOCK_SRC "/usr/src/gmock/gmock-all.cc")

if(NOT EXISTS ${GMOCK_SRC})
  unset(GMOCK_SRC)
endif(NOT EXISTS ${GMOCK_SRC})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gmock DEFAULT_MSG
  GMOCK_SRC)
