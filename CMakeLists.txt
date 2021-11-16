# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 2.6)

project(cta)


set(CMAKE_C_FLAGS "-fPIC -Wall -Wextra -Werror -Wno-unused-parameter")

# Please note that the -fms-extensions option should be dropped when this
# project gets to be compiled on a fully C++ 11 compliant compiler.  The option
# is being used to support anonymous structs and avoid errors similar to the
# following:
#
#   ClientSimulator.hpp:65: error: ISO C++ prohibits anonymous structs
set(CMAKE_CXX_FLAGS "-fPIC -pedantic -Wall -Wextra -Werror -Wno-unused-parameter -Wno-long-long -fstack-protector-all -Wnon-virtual-dtor")
#
# A maximalist error checking parameter combo has been suggested by S. Ponce and D. Come:
# to be tested once we have a stable compilation on CC7:
#-Wno-unused-parameter -Wlogical-op -Wfloat-equal -Wdeclaration-after- statement -Wundef -Wno-endif-labels -Wshadow -Wunsafe-loop- optimizations -Wpointer-arith -Wbad-function-cast -Wcast-align -Wwrite- strings -Wconversion -Wmissing-field-initializers -Wredundant-decls -Wnested-externs -Wunreachable-code -Winline -Wvariadic-macros -Wtraditional -Wmissing-prototypes -Wmissing-declarations -Wold-style- definition -Wc++-compat -Wstrict-prototypes -Wpadded -Wcast-qual -Wnon-virtual-dtor -Wlogical-op -Wmissing-declarations -Wsign-conversion -Wredundant-decls -Wold-style-cast -Wshadow

# Explicitly setting the C and C++ compiler flags for the RelWithDebInfo build
# in order to prevent the -O2 flag from being used.
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-g")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g")

set (BUILD_CMDLINE_ONLY FALSE)

execute_process(
  COMMAND ${CMAKE_C_COMPILER} -dumpversion
  OUTPUT_VARIABLE GCC_VERSION)

if (CMAKE_COMPILER_IS_GNUCC)
  if (GCC_VERSION VERSION_EQUAL 4.4.7)
    message(STATUS "Detected gcc = 4.4.7 - build cmdline only with c++0x")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
    set (BUILD_CMDLINE_ONLY TRUE)
  endif (GCC_VERSION VERSION_EQUAL 4.4.7)
  if (GCC_VERSION VERSION_GREATER 4.4.7)
    message(STATUS "Detected gcc > 4.4.7 - switching on support for c++1y")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y")
  endif (GCC_VERSION VERSION_GREATER 4.4.7)
endif (CMAKE_COMPILER_IS_GNUCC)

set (GCC_VERSION_GE_4_8_0 FALSE)
if (CMAKE_COMPILER_IS_GNUCC AND ((GCC_VERSION VERSION_GREATER 4.8.0) OR (GCC_VERSION VERSION_EQUAL 4.8.0)))
  set (GCC_VERSION_GE_4_8_0 TRUE)
endif (CMAKE_COMPILER_IS_GNUCC AND ((GCC_VERSION VERSION_GREATER 4.8.0) OR (GCC_VERSION VERSION_EQUAL 4.8.0)))

# To enable logging for testing purposes set -DENABLE_STDOUT_LOGGING=1
if (ENABLE_STDOUT_LOGGING)
  add_definitions (-DSTDOUT_LOGGING)
endif ()

# Generate the compilation variables, if needed
if (NOT DEFINED SKIP_UNIT_TESTS)
  message (STATUS "Setting SKIP_UNIT_TESTS to the value of 0")
  message (STATUS "Override with -DSKIP_UNIT_TESTS:STRING=1")
  set(SKIP_UNIT_TESTS 0)
else (NOT DEFINED SKIP_UNIT_TESTS)
  message (STATUS "Already set: SKIP_UNIT_TESTS=${SKIP_UNIT_TESTS}")
endif (NOT DEFINED SKIP_UNIT_TESTS)

#Load version information in all cases.
include(cmake/CTAVersions.cmake)

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

  set(CMAKE_DISABLE_SOURCE_CHANGES ON)
  set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
  list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
  include(GNUInstallDirs)
  set(CMAKE_INSTALL_PREFIX "/")

  include_directories(${PROJECT_SOURCE_DIR})
  include_directories(${CMAKE_BINARY_DIR})

  # OCCI support is on by default
  set (OCCI_SUPPORT ON)

  # Switch OCCI support off if using gcc 5.1 or higher
  #if (CMAKE_COMPILER_IS_GNUCC)
  #  if (GCC_VERSION VERSION_GREATER 5.0)
  #    message (STATUS "Detected gcc >= 5.1 - Switching off OCCI support")
  #    set (OCCI_SUPPORT OFF)
  #  endif (GCC_VERSION VERSION_GREATER 5.0)
  #endif (CMAKE_COMPILER_IS_GNUCC)

  if (OCCI_SUPPORT)
    find_package (oracle-instantclient REQUIRED)
  endif (OCCI_SUPPORT)

  install (CODE "message (STATUS \"Installing directory \$ENV{DESTDIR}/var/log/cta\")")
  install (CODE "file (MAKE_DIRECTORY \$ENV{DESTDIR}/var/log/cta)")

  #
  # Location of git submodule containing the XRootD SSI Protocol Buffer bindings
  #
  set(XRD_SSI_PB_DIR ${PROJECT_SOURCE_DIR}/xrootd-ssi-protobuf-interface)

  add_subdirectory(eos_cta)

  add_subdirectory(eos_grpc_client)

  add_subdirectory(migration)

  add_subdirectory(cmdline)
  add_subdirectory(xroot_plugins)

  add_subdirectory(catalogue)
  add_subdirectory(common)
  add_subdirectory(disk)
  add_subdirectory(mediachanger)
  add_subdirectory(objectstore)
  add_subdirectory(python)
  add_subdirectory(rdbms)
  add_subdirectory(scheduler)
  add_subdirectory(tapeserver)

  add_subdirectory(statistics)

  add_subdirectory(continuousintegration/orchestration/tests)

  add_subdirectory(cta-release)

  #Generate version information
  configure_file(${PROJECT_SOURCE_DIR}/version.hpp.in
    ${CMAKE_BINARY_DIR}/version.h)
ENDIF(DEFINED PackageOnly)

################################################################################
# Packaging step (replacing the maketar)
# See http://www.vtk.org/Wiki/CMakeUserUseRPMTools
################################################################################
if (${COMPILE_PACKAGING} STREQUAL "1")
  include(cmake/UseRPMToolsEnvironment.cmake)
  set(CPACK_SOURCE_PACKAGE_FILE_NAME
   "${PROJECT_NAME}-${CTA_VERSION}-${CTA_RELEASE}")
  message (STATUS
    "Setting package file name to: ${CPACK_SOURCE_PACKAGE_FILE_NAME}")
  set(CPACK_SOURCE_IGNORE_FILES "/.git/")
  include(CPack)
  include(cmake/UseRPMTools.cmake)
  if (RPMTools_FOUND)
    RPMTools_ADD_RPM_TARGETS(
      ${PROJECT_NAME} ${PROJECT_NAME}.spec.in)
  endif (RPMTools_FOUND)
endif (${COMPILE_PACKAGING} STREQUAL "1")

#add_custom_target(test test/castorUnitTests
#                    COMMENT "Not running the unit tests" VERBATIM)

configure_file(tests/valgrind.suppr tests/valgrind.suppr COPYONLY)
configure_file(tests/helgrind.suppr tests/helgrind.suppr COPYONLY)
set(VALGRIND valgrind)
set(VALGRIND_OPTS --leak-check=full --demangle=yes --gen-suppressions=all --show-reachable=yes --error-exitcode=1 --max-threads=1000)
set(VALGRIND_OPTS_W_SUPPR ${VALGRIND_OPTS} --suppressions=tests/valgrind.suppr)
string (REPLACE ";" " " VALGRIND_OPTS_STR "${VALGRIND_OPTS}")

set(HELGRIND_OPTS -v --demangle=yes --gen-suppressions=all --num-callers=25 --conflict-cache-size=10000000 --error-exitcode=1  --sim-hints=no-nptl-pthread-stackcache --max-threads=1000)
set(HELGRIND_OPTS_W_SUPPR ${HELGRIND_OPTS} --suppressions=tests/helgrind.suppr)
string (REPLACE ";" " " HELGRIND_OPTS_STR "${HELGRIND_OPTS}")

IF(NOT DEFINED PackageOnly)
  add_subdirectory(tests)
ENDIF(NOT DEFINED PackageOnly)

add_custom_target(fullunittests
  tests/cta-unitTests
  COMMAND tests/cta-unitTests-multiProcess
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} tests/cta-unitTests
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} tests/cta-unitTests
  COMMAND tests/cta-unitTests-multiProcess
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes tests/cta-unitTests-multiProcess
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} tests/cta-unitTests-multiProcess

  DEPENDS tests/cta-unitTests tests/cta-unitTests-multiProcess tests/valgrind.suppr tests/helgrind.suppr
  COMMENT "Running unit tests with memory leak and race conditions detection" VERBATIM)

add_custom_target(valgrind
  echo ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} tests/cta-unitTests
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} tests/cta-unitTests
  COMMAND echo ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes tests/cta-unitTests-multiProcess
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes tests/cta-unitTests-multiProcess

  DEPENDS tests/cta-unitTests tests/cta-unitTests-multiProcess tests/valgrind.suppr
  COMMENT "Running unit tests with memory leak detection" VERBATIM)

add_custom_target(helgrind
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} tests/cta-unitTests
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} tests/cta-unitTests
  COMMAND echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} tests/cta-unitTests-multiProcess
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} tests/cta-unitTests-multiProcess

  DEPENDS tests/cta-unitTests tests/cta-unitTests-multiProcess tests/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection" VERBATIM)

add_custom_target(shortunittests
  tests/cta-unitTests
  COMMAND tests/cta-unitTests-multiProcess

  DEPENDS tests/cta-unitTests tests/cta-unitTests-multiProcess
  COMMENT "Running unit tests" VERBATIM)

add_custom_target(helgrindBase
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=-OStoreDBPlusMockSchedulerTestVFS*:OStoreTestVFS*:OStoreDBPlusMockSchedulerTestVFS*:InMemory*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=-OStoreDBPlusMockSchedulerTestVFS*:OStoreTestVFS*:OStoreDBPlusMockSchedulerTestVFS*:InMemory*
  COMMAND echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests-multiProcess
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests-multiProcess

  DEPENDS tests/cta-unitTests tests/cta-unitTests-multiProcess tests/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (except slow sections)" VERBATIM)

add_custom_target(helgrindScheduler
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/Scheduler*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/Scheduler*

  DEPENDS tests/cta-unitTests tests/cta-unitTests-multiProcess tests/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (Mock scheduler section)" VERBATIM)

add_custom_target(helgrindOStoreDB
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=OStoreTestVFS*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=OStoreTestVFS*

  DEPENDS tests/cta-unitTests tests/cta-unitTests-multiProcess tests/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (Object store DB section)" VERBATIM)

add_custom_target(helgrindDataTransfer
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/DataTransferSessionTest*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/DataTransferSessionTest*

  DEPENDS tests/cta-unitTests tests/cta-unitTests-multiProcess tests/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (Data transfer section)" VERBATIM)

add_custom_target(helgrindInMemoryCatalogue
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=InMemory/*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q tests/cta-unitTests --gtest_filter=InMemory/*

  DEPENDS tests/cta-unitTests tests/cta-unitTests-multiProcess tests/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (In memory catalogue section)" VERBATIM)

add_custom_target(parallelHelgrind
  echo

  DEPENDS helgrindBase helgrindMockSechduler helgrindOStoreDB helgrindDataTransfer helgrindInMemoryCatalogue)
