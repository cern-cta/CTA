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

################################################################################
# Configure header file
################################################################################
configure_file(${CMAKE_SOURCE_DIR}/h/patchlevel.h.in
  ${CMAKE_BINARY_DIR}/h/patchlevel.h)

################################################################################
# Global rules and variables
################################################################################
set (CASTOR_DEST_BIN_DIR /usr/bin)
set (CASTOR_DEST_LIB_DIR /usr/lib64)
set (CASTOR_DEST_MAN_DIR /usr/share/man)

set (CASTOR_MAN_PAGE_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

include_directories(${CMAKE_BINARY_DIR})
include_directories(${CMAKE_BINARY_DIR}/h)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/h)

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --home OUTPUT_VARIABLE ORACLE_HOME)
string (REGEX REPLACE "\n" "" ORACLE_HOME ${ORACLE_HOME})
message (STATUS "ORACLE_HOME = '${ORACLE_HOME}'")
execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --bindir OUTPUT_VARIABLE ORACLE_BIN)
string (REGEX REPLACE "\n" "" ORACLE_BIN ${ORACLE_BIN})
message (STATUS "ORACLE_BIN = ${ORACLE_BIN}")
execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --libdir OUTPUT_VARIABLE ORACLE_LIBDIR)
string (REGEX REPLACE "\n" "" ORACLE_LIBDIR ${ORACLE_LIBDIR})
message (STATUS "ORACLE_LIBDIR = ${ORACLE_LIBDIR}")
execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --cppflags OUTPUT_VARIABLE ORACLE_CPPFLAGS)
string (REGEX REPLACE "\n" "" ORACLE_CPPFLAGS ${ORACLE_CPPFLAGS})
message (STATUS "ORACLE_CPPFLAGS = ${ORACLE_CPPFLAGS}")
execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --procinc OUTPUT_VARIABLE ORACLE_PROCINC)
string (REGEX REPLACE "\n" "" ORACLE_PROCINC ${ORACLE_PROCINC})
message (STATUS "ORACLE_PROCINC = ${ORACLE_PROCINC}")

function (CastorAddProC _name)
  add_custom_command (
    OUTPUT ${_name}.c DEPENDS ${_name}.pc
    COMMAND
      ORACLE_HOME=${ORACLE_HOME}
      LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH}:${ORACLE_LIBDIR}
      ${ORACLE_BIN}/proc
      iname=${CMAKE_CURRENT_SOURCE_DIR}/${_name}
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

function (CastorInstallManPage _name _section)
  install (FILES ${_name}.man
    DESTINATION ${CASTOR_DEST_MAN_DIR}/man${_section}
    PERMISSIONS ${CASTOR_MAN_PAGE_PERMS}
    RENAME ${_name}.${_section}castor)
endfunction ()

function (CastorInstallExeManPage _name)
  CastorInstallManPage (${_name} 1)
endfunction ()

function (CastorInstallSysManPage _name)
  CastorInstallManPage (${_name} 2)
endfunction ()

function (CastorInstallLibManPage _name)
  CastorInstallManPage (${_name} 3)
endfunction ()

function (CastorInstallFileManPage _name)
  CastorInstallManPage (${_name} 4)
endfunction ()

function (CastorInstallAdmManPage _name)
  CastorInstallManPage (${_name} 8)
endfunction ()

set (CASTOR_LOGROTATE_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

function (CastorInstallLogrotate _name)
  install (FILES ${_name}.logrotate
    DESTINATION /etc/logrotate.d
    PERMISSIONS ${CASTOR_LOGROTATE_PERMS}
    RENAME ${_name})
endfunction ()

set (CASTOR_SYSCONFIG_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

function (CastorInstallSysconfig _name)
  install (FILES ${_name}.sysconfig
    DESTINATION /etc/sysconfig
    PERMISSIONS ${CASTOR_SYSCONFIG_PERMS}
    RENAME ${_name})
endfunction ()

set (CASTOR_INITSCRIPT_PERMS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ             GROUP_EXECUTE
  WORLD_READ             WORLD_EXECUTE)

function (CastorInstallInitScript _name)
  install (FILES ${_name}.init
    DESTINATION /etc/init.d
    PERMISSIONS ${CASTOR_INITSCRIPT_PERMS}
    RENAME ${_name})
endfunction ()

set (CASTOR_CONFIG_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ)

function (CastorInstallConfigFile _name)
  install (FILES ${_name}CONFIG
    DESTINATION /etc/castor
    PERMISSIONS ${CASTOR_CONFIG_PERMS}
    RENAME ${_name}CONFIG.example)
endfunction ()

set (CMAKE_C_FLAGS
  "-pthread -fPIC -Wall -Wextra -Werror -Wno-unused-parameter")

if (APPLE)
  set (CMAKE_SHARED_LINKER_FLAGS
    "${CMAKE_SHARED_LINKER_FLAGS} -undefined dynamic_lookup")
else (APPLE)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
endif (APPLE)

set (CMAKE_CXX_FLAGS ${CMAKE_C_FLAGS})

set_property (DIRECTORY PROPERTY COMPILE_DEFINITIONS _LARGEFILE64_SOURCE)

# Generate the compilation variables, if needed
if(NOT DEFINED COMPILE_CLIENT)
  message(STATUS "Setting COMPILE_CLIENT to the value of 0")
  message(STATUS "Override with -DCOMPILE_CLIENT:STRING=1")
  set(COMPILE_CLIENT 0)
endif(NOT DEFINED COMPILE_CLIENT)

################################################################################
# Include the subdirectories of the project
################################################################################

add_subdirectory (castor)
add_subdirectory (common)
add_subdirectory (dlf)
add_subdirectory (ns)
add_subdirectory (rmc)
add_subdirectory (security)
add_subdirectory (tape)
add_subdirectory (upv)
add_subdirectory (vdqm)
add_subdirectory (vmgr)

################################################################################
# Packaging step (replacing the maketar)
# See http://www.vtk.org/Wiki/CMakeUserUseRPMTools
################################################################################
include(cmake/UseRPMToolsEnvironment.cmake)
set(CPACK_SOURCE_PACKAGE_FILE_NAME
 "${PROJECT_NAME}-${CASTOR_VERSION}-${CASTOR_RELEASE}${RPMTools_RPMBUILD_DIST}")
message(STATUS
  "Setting package file name to: ${CPACK_SOURCE_PACKAGE_FILE_NAME}")
include(CPack)
include(cmake/UseRPMTools.cmake)
if(RPMTools_FOUND)
  execute_process(COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_SOURCE_DIR}/${PROJECT_NAME}.spec.in.head
    ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in)
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
