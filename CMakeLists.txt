cmake_minimum_required (VERSION 2.6)

project(cta)

set(CMAKE_C_FLAGS "-fPIC -pedantic -Wall -Wextra -Werror -Wno-unused-parameter")

# Please note that the -fms-extensions option should be dropped when this
# project gets to be compiled on a fully C++ 11 compliant compiler.  The option
# is being used to support anonymous structs and avoid errors similar to the
# following:
#
#   ClientSimulator.hpp:65: error: ISO C++ prohibits anonymous structs
set(CMAKE_CXX_FLAGS "-fPIC -pedantic -Wall -Wextra -Werror -Wno-unused-parameter -Wno-long-long -std=c++0x -fms-extensions -fstack-protector-all")


# Explicitly setting the C and C++ compiler flags for the RelWithDebInfo build
# in order to prevent the -O2 flag from being used.
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-g")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g")

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
  include(cmake/CTAInstallationFunctions.cmake)
  include(cmake/CTAVersions.cmake)
  include(GNUInstallDirs)
  set(CMAKE_INSTALL_PREFIX "/")

  include_directories(${PROJECT_SOURCE_DIR})
  include_directories(${CMAKE_BINARY_DIR})

  add_subdirectory(catalogue)
  add_subdirectory(cmdline)
  add_subdirectory(common)
  add_subdirectory(objectstore)
  add_subdirectory(nameserver)
  add_subdirectory(remotens)
  add_subdirectory(scheduler)
  add_subdirectory(tapeserver)
  add_subdirectory(tests)
  add_subdirectory(xroot_plugins)
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
add_custom_target(unittests
  tests/unitTests
  COMMAND valgrind --track-fds=yes --leak-check=full --demangle=yes --gen-suppressions=all --show-reachable=yes --error-exitcode=1 --suppressions=tests/valgrind.suppr tests/unitTests
  COMMAND valgrind --tool=helgrind -v --demangle=no --conflict-cache-size=30000000 --error-exitcode=1 tests/unitTests 

  DEPENDS tests/unittests tests/valgrind.suppr
  COMMENT "Running unit tests" VERBATIM)
