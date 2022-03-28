# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2013-2022 CERN
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
cmake_minimum_required (VERSION 3.17)

###########################################################################
# dependancies
###########################################################################
IF(DEFINED PackageOnly)
ELSE(DEFINED PackageOnly)
  find_package(GTest REQUIRED)
ENDIF(DEFINED PackageOnly)
enable_testing()
set(CTEST_OUTPUT_ON_FAILURE 1)

###########################################################################
# main part
###########################################################################

# Prepare tests file list
#set(TEST_FILES "")
#set(TEST_LIBS "")

# Add parts first in dependency order
add_subdirectory(SCSI)
add_subdirectory(drive)
add_subdirectory(system)
add_subdirectory(file)
add_subdirectory(daemon)
add_subdirectory(RAO)

# .. and of course, the tests (last to use the variable definition)
add_subdirectory(test)
