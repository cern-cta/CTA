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
add_subdirectory(SCSI)
add_subdirectory(Drive)
add_subdirectory(System)
add_subdirectory(Exception)
add_subdirectory(Utils)
add_subdirectory(File)
add_subdirectory(Daemon)

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

