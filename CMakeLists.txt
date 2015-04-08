cmake_minimum_required (VERSION 2.6)

project(cta)

set(CMAKE_C_FLAGS "-fPIC -pedantic -Wall -Wextra -Werror -Wno-unused-parameter")
set(CMAKE_CXX_FLAGS "-fPIC -pedantic -Wall -Wextra -Werror -Wno-unused-parameter -Wno-long-long")


# Explicitly setting the C and C++ compiler flags for the RelWithDebInfo build
# in order to prevent the -O2 flag from being used.
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

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

add_subdirectory(middletier)
add_subdirectory(objectstore)
add_subdirectory(objectstore_middletier)
add_subdirectory(tests)
add_subdirectory(xroot_clients)
add_subdirectory(xroot_plugins)
