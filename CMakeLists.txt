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

# With the exception of shared-library plugins, the CASTOR rpms only install the
# /usr/lib64/libcastor*.so symbolic links for libraries used by end-user
# developers.  Therefore the locations of the internal CASTOR libraries required
# by tapeserved and not by end-user developers need to be imported into cmake.
add_library(castorlegacymsg SHARED IMPORTED)
set_target_properties(castorlegacymsg PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastorlegacymsg.so.2.1)
add_library(castorserver SHARED IMPORTED)
set_target_properties(castorserver PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastorserver.so.2.1)
add_library(castortapegatewayprotocol SHARED IMPORTED)
set_target_properties(castortapegatewayprotocol PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastortapegatewayprotocol.so.2.1)

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

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

add_subdirectory(cmdline)
add_subdirectory(common)
add_subdirectory(objectstore)
add_subdirectory(nameserver)
add_subdirectory(remotens)
add_subdirectory(scheduler)
add_subdirectory(tapeserver)
add_subdirectory(tests)
add_subdirectory(xroot_plugins)
