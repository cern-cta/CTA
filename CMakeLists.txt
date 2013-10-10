# ----------------------------------------------------------------------
# File: CMakeLists.txt
# ----------------------------------------------------------------------

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
# In source build forbidding
###########################################################################
cmake_minimum_required (VERSION 2.6)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

###########################################################################
# project definition section
###########################################################################
project(tape-server)
set(tape-server_VERSION_MAJOR "0.0.0")
IF(DEFINED VCS_VERSION)
  message("VCS version present: ${VCS_VERSION}")
  set(tape-server_VERSION_MINOR ${VCS_VERSION})
ELSE(DEFINED VCS_VERSION)
  set(tape-server_VERSION_MINOR "1")
ENDIF(DEFINED VCS_VERSION)
LIST(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

###########################################################################
# compiler options
###########################################################################
set (CMAKE_CXX_FLAGS "-g3 -Wall -Werror -pedantic")

###########################################################################
# dependancies
###########################################################################
find_package(GTest REQUIRED)

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
# add a target to generate API documentation with Doxygen
find_package(Doxygen)
if(DOXYGEN_FOUND)
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
  add_custom_target(doxygen
    ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating API documentation with Doxygen" VERBATIM
)
endif(DOXYGEN_FOUND)

INCLUDE(cmake/UseLATEX.cmake)

add_subdirectory(Documentation)

###########################################################################
# Packaging step (replacing the maketar)
# See http://www.vtk.org/Wiki/CMakeUserUseRPMTools
###########################################################################
SET(CPACK_SOURCE_PACKAGE_FILE_NAME "${PROJECT_NAME}-${tape-server_VERSION_MAJOR}-${tape-server_VERSION_MINOR}")
INCLUDE(CPack)
INCLUDE(cmake/UseRPMTools.cmake)
IF(RPMTools_FOUND)
  RPMTools_ADD_RPM_TARGETS(tape-server ${CMAKE_SOURCE_DIR}/tape-server.spec.in)
ENDIF(RPMTools_FOUND)

