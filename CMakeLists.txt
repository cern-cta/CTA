# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Eric Cano - CERN
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

##########################################################################
# Version Definition
##########################################################################
set(VERSION_MAJOR "0" )
set(VERSION_MINOR "0")
set(VERSION_PATCH "0")
set(VERSION_TWEAK "1")

###########################################################################
# In source build forbidding
###########################################################################
cmake_minimum_required (VERSION 2.6)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

###########################################################################
# project definition section
###########################################################################
project (CERN_Tape)
LIST(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

###########################################################################
# dependancies
###########################################################################
find_package(GTest REQUIRED)

enable_testing()
set(CTEST_OUTPUT_ON_FAILURE 1)

###########################################################################
# main part
###########################################################################

# Add parts first in dependency order
add_subdirectory(SCSI)
add_subdirectory(Drive)
add_subdirectory(system)
add_subdirectory(Exception)

# .. and then tools, tests, and daemon
# add_subdirectory(tools)
# add_subdirectory(deamon)

# .. and of course, the tests
add_subdirectory(test)

###########################################################################
# compiler options
###########################################################################
set (CMAKE_CXX_FLAGS “${CMAKE_CXX_FLAGS} -Wall -Werror -pedantic”)


###########################################################################
# documentation
###########################################################################
# add a target to generate API documentation with Doxygen
find_package(Doxygen)
if(DOXYGEN_FOUND)
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
  add_custom_target(doc
    ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating API documentation with Doxygen" VERBATIM
)
endif(DOXYGEN_FOUND)