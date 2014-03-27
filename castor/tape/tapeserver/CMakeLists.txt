# ************************************************************************
# * Tape Server                                                          *
# * Copyright (C) 2013 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************

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
add_subdirectory(client)
add_subdirectory(SCSI)
add_subdirectory(drive)
add_subdirectory(system)
add_subdirectory(exception)
add_subdirectory(utils)
add_subdirectory(file)
add_subdirectory(daemon)
add_subdirectory(threading)

# .. and of course, the tests (last to use the variable definition)
add_subdirectory(test)

###########################################################################
# documentation
###########################################################################
#TODO
#IF (NOT DEFINED PackageOnly)
#  INCLUDE(${PROJECT_SOURCE_DIR}/cmake/UseLATEX.cmake)
#  add_subdirectory(Documentation)
#ENDIF (NOT DEFINED PackageOnly)

