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

# Explicitly setting the C and C++ compiler flags for the RelWithDebInfo build
# in order to prevent the -O2 flag from being used.
#
# The CASTOR executables are not running short of CPU cycles.  The extra
# effort required to debug released binaries compiled with the -O2 option is
# therefore not justified.
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-g")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g")

IF(NOT CMAKE_BUILD_TYPE STREQUAL "")
  # If the user specifies -DCMAKE_BUILD_TYPE on the command line, take their definition and dump it in the cache
  message(STATUS "Setting build type to ${CMAKE_BUILD_TYPE} as requested.")
  SET(CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE} CACHE STRING "Choose the type of build.")
ELSE()
  # log choosen default (RelWithDebInfo) and set it
  message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
  message (STATUS "Override with -DCMAKE_BUILD_TYPE:STRING=Debug")  
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  # this command is not yet available in SLC6's cmake 2.6
  # set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
ENDIF(NOT CMAKE_BUILD_TYPE STREQUAL "")

if ("${CMAKE_SOURCE_DIR}" MATCHES "${CMAKE_BINARY_DIR}")
  message (SEND_ERROR "In source building not supported. Please run something like: \"mkdir ../build; ( src=`pwd`; cd ../build; cmake $src ); make -C ../build\"")
  message (SEND_ERROR "Now that you reached that point you will unfortnately have to cleanup your source directory: \"rm -rf CMakeFiles/ CMakeCache.txt\". Sorry!")
  return()
endif ("${CMAKE_SOURCE_DIR}" MATCHES "${CMAKE_BINARY_DIR}")
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
include(cmake/CASTORVersion.cmake)


# Generate the compilation variables, if needed
if (NOT DEFINED COMPILE_SERVER)
  message (STATUS "Setting COMPILE_SERVER to the value of 1")
  message (STATUS "Override with -DCOMPILE_SERVER:STRING=0")
  set(COMPILE_SERVER 1)
else (NOT DEFINED COMPILE_SERVER)
  message (STATUS "Already set: COMPILE_SERVER=${COMPILE_SERVER}")
endif (NOT DEFINED COMPILE_SERVER)

# Generate the compilation variables, if needed
if (NOT DEFINED SKIP_UNIT_TESTS)
  message (STATUS "Setting SKIP_UNIT_TESTS to the value of 0")
  message (STATUS "Override with -DSKIP_UNIT_TESTS:STRING=1")
  set(SKIP_UNIT_TESTS 0)
else (NOT DEFINED SKIP_UNIT_TESTS)
  message (STATUS "Already set: SKIP_UNIT_TESTS=${SKIP_UNIT_TESTS}")
endif (NOT DEFINED SKIP_UNIT_TESTS)

IF(DEFINED PackageOnly)
  message (STATUS "Running CMake in package-only mode")
   set(COMPILE_PACKAGING "1") 
ELSE(DEFINED PackageOnly)
  message (STATUS "Running in full configuration mode.")
  message (STATUS "Override with -DPackageOnly:Bool=true")
  # Generate the compilation variables, if needed
  if (NOT DEFINED COMPILE_PACKAGING)
    message (STATUS "Setting COMPILE_PACKAGING to the value of 1")
    message (STATUS "Override with -DCOMPILE_PACKAGING:STRING=0")
    set(COMPILE_PACKAGING 1)
  else (NOT DEFINED COMPILE_PACKAGING)
    message (STATUS "Already set: COMPILE_PACKAGING=${COMPILE_PACKAGING}")
  endif (NOT DEFINED COMPILE_PACKAGING)

  ################################################################################
  # Configure header file
  ################################################################################
  configure_file(${CMAKE_SOURCE_DIR}/h/patchlevel.h.in
    ${CMAKE_BINARY_DIR}/h/patchlevel.h)
  
  include(cmake/CASTORInstallationFunctions.cmake)
  find_package (uuid REQUIRED)
  find_package (python REQUIRED)
  
  include_directories(${CMAKE_BINARY_DIR})
  include_directories(${CMAKE_BINARY_DIR}/h)
  include_directories(${CMAKE_SOURCE_DIR})
  include_directories(${CMAKE_SOURCE_DIR}/h)
  
  if (${COMPILE_SERVER} STREQUAL "1")
    find_package (oracle REQUIRED)
    
    function (CastorAddProC _name)
      add_custom_command (
        OUTPUT ${_name}.c DEPENDS ${_name}.pc
        COMMAND
          ORACLE_HOME=${ORACLE_HOME}
          LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH}:${ORACLE_LIBDIR}
          ${ORACLE_BIN}/proc
          iname=${CMAKE_CURRENT_SOURCE_DIR}/${_name}.pc
          oname=${CMAKE_CURRENT_BINARY_DIR}/${_name}.c
          ${ORACLE_PROCINC}
          include=${CMAKE_SOURCE_DIR}/h threads=yes
          char_map=string
          parse=full
          prefetch=1000)
    
      # Append the oracle specific compilation flags and also use -Wno-error to
      # override the default -Werror compilation rule in the case of source files
      # produced by the Pro*C precompiler.  Such files contain generated code that
      # generates warnings.
      set_property (SOURCE ${_name}.c
        PROPERTY COMPILE_FLAGS "${ORACLE_CPPFLAGS} -Wno-error")
    endfunction ()
  endif (${COMPILE_SERVER} STREQUAL "1")
  
  set (CMAKE_C_FLAGS "-fPIC -Wall -Wextra -Werror -Wno-unused-parameter")
  
  if (APPLE)
    set (CMAKE_SHARED_LINKER_FLAGS
      "${CMAKE_SHARED_LINKER_FLAGS} -undefined dynamic_lookup")
  else (APPLE)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
  endif (APPLE)
  
  set (CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wno-virtual-dtor")
  
  set_property (DIRECTORY PROPERTY COMPILE_DEFINITIONS _LARGEFILE64_SOURCE)
  
  ##############################################################################
  # Rules to install the shared log and spool directories
  ##############################################################################
  CastorInstallDir (/var/log/castor)
  CastorInstallDir (/var/log/spool/castor)
  
  ##############################################################################
  # Include the subdirectories of the project
  ##############################################################################
  
  # Some directories have client components, and get included unconditionnaly
  # inside them, we can filter further down what we need or not.
  # The ones with only server components can already be filtered out for the 
  # clients builds here.
  
  add_subdirectory (castor)
  add_subdirectory (client/src/stager)
  add_subdirectory (common)
  add_subdirectory (dlf)
  if (${COMPILE_SERVER} STREQUAL "1")
    add_subdirectory (gridftp2)
    add_subdirectory (ceph)
  endif (${COMPILE_SERVER} STREQUAL "1")
  add_subdirectory (h)
  if (${COMPILE_SERVER} STREQUAL "1")
    add_subdirectory (hsmtools)
    add_subdirectory (monitoring)
  endif (${COMPILE_SERVER} STREQUAL "1")
  add_subdirectory (ns)
  add_subdirectory (rfio)
  add_subdirectory (rmc)
  add_subdirectory (security)
  add_subdirectory (shlib)
  if (${COMPILE_SERVER} STREQUAL "1")
    add_subdirectory (tapebridge)
    add_subdirectory (traffic-shaping)
  endif (${COMPILE_SERVER} STREQUAL "1")
  add_subdirectory (upv)
  add_subdirectory (vdqm)
  add_subdirectory (vmgr)
  if (${COMPILE_SERVER} STREQUAL "1")
    add_subdirectory (xrootd)
  endif (${COMPILE_SERVER} STREQUAL "1")
  add_subdirectory (tape)
  if (${COMPILE_SERVER} STREQUAL "1")
    add_subdirectory (rtcopy)
    add_subdirectory (test)
  endif (${COMPILE_SERVER} STREQUAL "1")
ENDIF(DEFINED PackageOnly)



################################################################################
# Packaging step (replacing the maketar)
# See http://www.vtk.org/Wiki/CMakeUserUseRPMTools
################################################################################
if (${COMPILE_PACKAGING} STREQUAL "1")
  include(cmake/UseRPMToolsEnvironment.cmake)
  set(CPACK_SOURCE_PACKAGE_FILE_NAME
   "${PROJECT_NAME}-${CASTOR_VERSION}-${CASTOR_RELEASE}")
  message (STATUS
    "Setting package file name to: ${CPACK_SOURCE_PACKAGE_FILE_NAME}")
  set(CPACK_SOURCE_IGNORE_FILES "/.git/")
  include(CPack)
  include(cmake/UseRPMTools.cmake)
  if (RPMTools_FOUND)
    message (STATUS "Generating spec file from debian package structure...")
    execute_process (
      COMMAND ./makespec.py
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      OUTPUT_FILE ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in
      OUTPUT_VARIABLE result)
    if (NOT ${result} STREQUAL "")
       string (REGEX REPLACE "\n$" "" result ${result})
      message (STATUS "${result}")
    endif (NOT ${result} STREQUAL "")
    RPMTools_ADD_RPM_TARGETS(
      ${PROJECT_NAME} ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in)
  endif (RPMTools_FOUND)
endif (${COMPILE_PACKAGING} STREQUAL "1")

#add_custom_target(test test/castorUnitTests
#                    COMMENT "Not running the unit tests" VERBATIM)

configure_file(test/castorThreadedUnitTests.supp test/castorThreadedUnitTests.supp COPYONLY)
configure_file(test/VCastorUnitTests.supp test/VCastorUnitTests.supp COPYONLY)
add_custom_target(test test/castorUnitTests
                    #  
                    COMMAND valgrind --track-fds=yes --leak-check=full --demangle=no --gen-suppressions=all --show-reachable=yes --error-exitcode=1 --suppressions=test/VCastorUnitTests.supp test/castorUnitTests
                    COMMAND test/castorThreadedUnitTests
                    COMMAND valgrind --track-fds=yes --leak-check=full --show-reachable=yes --error-exitcode=1 --suppressions=test/VCastorUnitTests.supp test/castorThreadedUnitTests
                    COMMAND valgrind --tool=helgrind -v --demangle=no --conflict-cache-size=30000000 --error-exitcode=1 --suppressions=test/castorThreadedUnitTests.supp test/castorThreadedUnitTests
                    COMMAND test/castorMultiprocessUnitTests
                    COMMAND valgrind --tool=helgrind --error-exitcode=1 --suppressions=test/castorThreadedUnitTests.supp test/castorMultiprocessUnitTests 
                    DEPENDS test/castorUnitTests test/castorThreadedUnitTests test/castorMultiprocessUnitTests test/castorThreadedUnitTests.supp
                    COMMENT "Running unit tests" VERBATIM)

