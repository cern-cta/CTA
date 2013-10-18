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

################################################################################
# Project definition section - In source build forbidding
################################################################################
cmake_minimum_required (VERSION 2.6)

project(castor)
if("${CMAKE_SOURCE_DIR}" MATCHES "${CMAKE_BINARY_DIR}")
  message(SEND_ERROR "In source building not supported. Please run something like: \"mkdir ../build; ( src=`pwd`; cd ../build; cmake $src ); make -C ../build\"")
  message(SEND_ERROR "Now that you reached that point you will unfortnately have to cleanup your source directory: \"rm -rf CMakeFiles/ CMakeCache.txt\". Sorry!")
  return()
endif("${CMAKE_SOURCE_DIR}" MATCHES "${CMAKE_BINARY_DIR}")
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
include(cmake/CASTORVersion.cmake)
find_package(Oracle)

################################################################################
# Configure header file
################################################################################
configure_file(${CMAKE_SOURCE_DIR}/h/patchlevel.h.in ${CMAKE_BINARY_DIR}/h/patchlevel.h)
include_directories(${CMAKE_BINARY_DIR}/h)
include_directories(${CMAKE_SOURCE_DIR}/h})

################################################################################
# Global rules and variables
################################################################################

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

# Generate the compilation variables, if needed
if(NOT DEFINED COMPILE_STK)
  message(STATUS "Setting COMPILE_STK to the default value of 0")
  message(STATUS "Override with -DCOMPILE_STK:STRING=1")
  set(COMPILE_STK 0)
endif(NOT DEFINED COMPILE_STK)

if(NOT DEFINED COMPILE_CLIENT)
  message(STATUS "Setting COMPILING_CLIENT to the value of 0")
  message(STATUS "Override with -DCOMPILING_CLIENT:STRING=1")
  set(COMPILE_CLIENT 0)
endif(NOT DEFINED COMPILE_CLIENT)

################################################################################
# Include the subdirectories of the project
################################################################################

add_subdirectory (common)
add_subdirectory (dlf)
add_subdirectory (rmc)
add_subdirectory (tape)
add_subdirectory (upv)
add_subdirectory (vdqm)
add_subdirectory (vmgr)

################################################################################
# Packaging step (replacing the maketar)
# See http://www.vtk.org/Wiki/CMakeUserUseRPMTools
################################################################################
set(CPACK_SOURCE_PACKAGE_FILE_NAME
  "${PROJECT_NAME}-${CASTOR_VERSION}-${CASTOR_RELEASE}")
include(CPack)
include(cmake/UseRPMTools.cmake)
if(RPMTools_FOUND)
  execute_process(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/${PROJECT_NAME}.spec.in.head  ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in)
  message(STATUS "Generating spec file from debian package structure...")
  execute_process(
    COMMAND ./makespec.in.sh ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in 2>&1
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE result)
  if(NOT ${result} STREQUAL "")
    message(STATUS "${result}")
  endif(NOT ${result} STREQUAL "")
  RPMTools_ADD_RPM_TARGETS(
    ${PROJECT_NAME} ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in)
endif(RPMTools_FOUND)
