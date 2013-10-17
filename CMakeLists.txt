#                      CMakeLists.txt
#
# This file is part of the Castor project.
# See http://castor.web.cern.ch/castor
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# Steven.Murray@cern.ch Eric.Cano@cern.ch
#

###########################################################################
# In source build forbidding
###########################################################################
CMAKE_MINIMUM_REQUIRED (VERSION 2.6)

###########################################################################
# project definition section
###########################################################################
PROJECT(castor)
IF("${CMAKE_SOURCE_DIR}" MATCHES "${CMAKE_BINARY_DIR}")
  MESSAGE(SEND_ERROR "In source building not supported. Please run something like: \"mkdir ../build; ( src=`pwd`; cd ../build; cmake $src ); make -C ../build\"")
  MESSAGE(SEND_ERROR "Now that you reached that point you will unfortnately have to cleanup your source directory: \"rm -rf CMakeFiles/ CMakeCache.txt\". Sorry!")
  RETURN()
ENDIF("${CMAKE_SOURCE_DIR}" MATCHES "${CMAKE_BINARY_DIR}")
SET(CMAKE_DISABLE_SOURCE_CHANGES ON)
SET(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
LIST(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
INCLUDE(cmake/CASTORVersion.cmake)
FIND_PACKAGE(Oracle)

###########################################################################
# Configure header file
###########################################################################
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/h/patchlevel.h.in ${CMAKE_BINARY_DIR}/h/patchlevel.h)
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/h)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/h})

###########################################################################
# Include the subdirectories of the project
###########################################################################

set(CASTOR_DEST_BIN_DIR /usr/bin)
set(CASTOR_DEST_LIB_DIR /usr/lib64)
set(CASTOR_DEST_MAN_DIR /usr/shar/man)

# All common variables and values to be used by all CMakeLists.txt files should
# be specified here
set (CMAKE_C_FLAGS
  "-pthread -fPIC -Wall -Wextra -Werror -Wno-unused-parameter")

if(APPLE)
  set (CMAKE_SHARED_LINKER_FLAGS
    "${CMAKE_SHARED_LINKER_FLAGS} -undefined dynamic_lookup")
else(APPLE)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
endif(APPLE)

set (CMAKE_CXX_FLAGS ${CMAKE_C_FLAGS})

set_property ( DIRECTORY PROPERTY COMPILE_DEFINITIONS _LARGEFILE64_SOURCE)

add_subdirectory (common)
add_subdirectory (dlf)
add_subdirectory (tape)
add_subdirectory (upv)
add_subdirectory (vmgr)

###########################################################################
# Packaging step (replacing the maketar)
# See http://www.vtk.org/Wiki/CMakeUserUseRPMTools
###########################################################################
SET(CPACK_SOURCE_PACKAGE_FILE_NAME "${PROJECT_NAME}-${CASTOR_VERSION}-${CASTOR_RELEASE}")
INCLUDE(CPack)
INCLUDE(cmake/UseRPMTools.cmake)
IF(RPMTools_FOUND)
  EXECUTE_PROCESS(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/${PROJECT_NAME}.spec.in.head  ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in)
  MESSAGE(STATUS "Generating spec file from debian pacakge structure...")
  EXECUTE_PROCESS(COMMAND ./makespec.in.sh ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in 2>&1
       WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
       OUTPUT_VARIABLE result)
  IF(NOT ${result} STREQUAL "")
    MESSAGE(STATUS "${result}")
  ENDIF(NOT ${result} STREQUAL "")
  RPMTools_ADD_RPM_TARGETS(${PROJECT_NAME} ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in)
ENDIF(RPMTools_FOUND)


