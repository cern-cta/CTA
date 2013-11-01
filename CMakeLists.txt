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
set (CASTOR_DEST_INCLUDE_DIR /usr/include)
set (CASTOR_DEST_PERL_LIBDIR /usr/lib/perl/CASTOR)

find_package (uuid REQUIRED)

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/pycompile.py --libs
  OUTPUT_VARIABLE CASTOR_PYTHON_LIBS)
string(REGEX REPLACE "\n" "" CASTOR_PYTHON_LIBS ${CASTOR_PYTHON_LIBS})
message(STATUS "CASTOR_PYTHON_LIBS = '${CASTOR_PYTHON_LIBS}'")
execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/pycompile.py --inc
  OUTPUT_VARIABLE CASTOR_PYTHON_INC)
string(REGEX REPLACE "\n" "" CASTOR_PYTHON_INC ${CASTOR_PYTHON_INC})
message(STATUS "CASTOR_PYTHON_INC = '${CASTOR_PYTHON_INC}'")
execute_process (
    COMMAND python -c "from distutils import sysconfig; print sysconfig.get_python_lib()"
  OUTPUT_VARIABLE CASTOR_DEST_PYTHON_LIBDIR)
string(REGEX REPLACE "\n" "" CASTOR_DEST_PYTHON_LIBDIR ${CASTOR_DEST_PYTHON_LIBDIR})
message(STATUS "CASTOR_DEST_PYTHON_LIBDIR = '${CASTOR_DEST_PYTHON_LIBDIR}'")

set (CASTOR_BIN_SCRIPT_PERMS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ             OWNER_EXECUTE
  WORLD_READ             WORLD_EXECUTE)

set (CASTOR_ETC_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ            )

set (CASTOR_ETC_CRON_D_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ            )

set (CASTOR_ETC_XINETD_D_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ            )

set (CASTOR_HEADER_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_MAN_PAGE_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_SYSCONFIG_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

include_directories(${CMAKE_BINARY_DIR})
include_directories(${CMAKE_BINARY_DIR}/h)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/h)

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

function (CastorInstallDir _name)
  install (CODE "FILE(MAKE_DIRECTORY \$ENV{DESTDIR}${_name})")
endfunction ()

function (CastorInstallEtcCronD _name)
  install (FILES ${_name}.cron_d
    DESTINATION /etc/cron.d
    PERMISSIONS ${CASTOR_ETC_CRON_D_PERMS}
    RENAME ${_name})
endfunction ()

function (CastorInstallEtcExample _name)
  install (FILES ${_name}.etc
    DESTINATION /etc
    PERMISSIONS ${CASTOR_ETC_PERMS}
    RENAME ${_name}.example)
endfunction ()

function (CastorInstallEtcXinetdD _name)
  install (FILES ${_name}.xinetd_d
    DESTINATION /etc/xinetd.d
    PERMISSIONS ${CASTOR_ETC_XINETD_D_PERMS}
    RENAME ${_name})
endfunction ()

function (CastorInstallScript _name)
  install (FILES ${_name}
    DESTINATION ${CASTOR_DEST_BIN_DIR}
    PERMISSIONS ${CASTOR_BIN_SCRIPT_PERMS})
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

function (CastorInstallSysconfigExample _name)
  install (FILES ${_name}.sysconfig
    DESTINATION /etc/sysconfig
    PERMISSIONS ${CASTOR_SYSCONFIG_PERMS}
    RENAME ${_name}.example)
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

function (CastorInstallConfigFileLowercase _name)
  install (FILES ${_name}
    DESTINATION /etc/castor
    PERMISSIONS ${CASTOR_CONFIG_PERMS}
    RENAME ${_name}.example)
endfunction ()

set (CASTOR_SCRIPT_PERMS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ             GROUP_EXECUTE
  WORLD_READ             WORLD_EXECUTE)

set (CASTOR_PYTHON_LIB_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ            
  WORLD_READ            )

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
if(NOT DEFINED COMPILE_SERVER)
  message(STATUS "Setting COMPILE_SERVER to the value of 1")
  message(STATUS "Override with -DCOMPILE_SERVER:STRING=0")
  set(COMPILE_SERVER 1)
endif(NOT DEFINED COMPILE_SERVER)

# Generate the compilation variables, if needed
if(NOT DEFINED COMPILE_PACKAGING)
  message(STATUS "Setting COMPILE_PACKAGING to the value of 1")
  message(STATUS "Override with -DCOMPILE_PACKAGING:STRING=0")
  set(COMPILE_PACKAGING 1)
endif(NOT DEFINED COMPILE_PACKAGING)

################################################################################
# Rules to install the shared log and spool directories
################################################################################
CastorInstallDir (/var/log/castor)
CastorInstallDir (/var/log/spool/castor)

################################################################################
# Include the subdirectories of the project
################################################################################

# Some directories have client components, and get included unconditionnaly
# inside them, we can filter further down what we need or not.
# The ones with only server components can already be filtered out for the 
# clients builds here.

add_subdirectory (castor)
add_subdirectory (client/src/stager)
add_subdirectory (common)
add_subdirectory (config)
add_subdirectory (dlf)
add_subdirectory (gridftp2)
add_subdirectory (h)
add_subdirectory (hsmtools)
add_subdirectory (ns)
add_subdirectory (rfio)
add_subdirectory (rmc)
add_subdirectory (security)
add_subdirectory (shlib)
add_subdirectory (tapebridge)
add_subdirectory (upv)
add_subdirectory (vdqm)
add_subdirectory (vmgr)
add_subdirectory (xrootd)
if(${COMPILE_SERVER} STREQUAL "1")
  add_subdirectory (tape)
endif(${COMPILE_SERVER} STREQUAL "1")

################################################################################
# Packaging step (replacing the maketar)
# See http://www.vtk.org/Wiki/CMakeUserUseRPMTools
################################################################################
if(${COMPILE_PACKAGING} STREQUAL "1")
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
      string(REGEX REPLACE "\n$" "" result ${result})
      message(STATUS "${result}")
    endif(NOT ${result} STREQUAL "")
    RPMTools_ADD_RPM_TARGETS(
      ${PROJECT_NAME} ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in)
  endif(RPMTools_FOUND)
endif(${COMPILE_PACKAGING} STREQUAL "1")
