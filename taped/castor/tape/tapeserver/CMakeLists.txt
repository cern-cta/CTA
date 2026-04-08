# SPDX-FileCopyrightText: 2013 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
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
