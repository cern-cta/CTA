# From, https://raw.githubusercontent.com/Kitware/CMake/master/Modules/FindProtobuf.cmake
# cut down to solve our problem and nothing more
#=============================================================================
# Copyright 2009 Kitware, Inc.
# Copyright 2009-2011 Philip Lowman <philip@yhbt.com>
# Copyright 2008 Esben Mose Hansen, Ange Optimization ApS
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================
# (To distribute this file outside of CMake, substitute the full
#  License text for the above reference.)

set(PROTOBUF3_ROOT /usr)
#set(PROTOBUF3_ROOT /opt/eos)

if (ALMA9)
  set(PROTOBUF3_RPATH ${PROTOBUF3_ROOT}/lib64)
  message(STATUS "PROTOBUF3_RPATH=${PROTOBUF3_RPATH}")

  set(PROTOBUF3_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR})

  find_program(PROTOBUF3_PROTOC3_EXECUTABLE
      NAMES ${PROTOBUF3_ROOT}/bin/protoc
      DOC "Version 3 of The Google Protocol Buffers Compiler"
  )
  message(STATUS "protoc is at ${PROTOBUF3_PROTOC3_EXECUTABLE} ")

  find_path(PROTOBUF3_INCLUDE_DIRS
    google/protobuf/message.h
    PATHS ${PROTOBUF3_ROOT}/include
    NO_DEFAULT_PATH)
  message(STATUS "PROTOBUF3_INCLUDE_DIRS=${PROTOBUF3_INCLUDE_DIRS}")
else()
  set(PROTOBUF3_RPATH ${PROTOBUF3_ROOT}/lib64/protobuf3)
  message(STATUS "PROTOBUF3_RPATH=${PROTOBUF3_RPATH}")

  set(PROTOBUF3_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR})

  find_program(PROTOBUF3_PROTOC3_EXECUTABLE
      NAMES ${PROTOBUF3_ROOT}/bin/protoc3
      DOC "Version 3 of The Google Protocol Buffers Compiler"
  )
  message(STATUS "protoc is at ${PROTOBUF3_PROTOC3_EXECUTABLE} ")

  find_path(PROTOBUF3_INCLUDE_DIRS
    google/protobuf/message.h
    PATHS ${PROTOBUF3_ROOT}/include/protobuf3
    NO_DEFAULT_PATH)
  message(STATUS "PROTOBUF3_INCLUDE_DIRS=${PROTOBUF3_INCLUDE_DIRS}")
endif()

find_library(PROTOBUF3_LIBRARIES
  NAME protobuf
  PATHS ${PROTOBUF3_RPATH}
  NO_DEFAULT_PATH)
message(STATUS "PROTOBUF3_LIBRARIES=${PROTOBUF3_LIBRARIES}")

function(PROTOBUF3_GENERATE_CPP SRCS HDRS)
  if(NOT ARGN)
    message(SEND_ERROR "Error: PROTOBUF3_GENERATE_CPP() called without any proto files")
    return()
  endif()

  set(_protobuf_include_path -I ${PROTOBUF3_INCLUDE_PATH})

  set(${SRCS})
  set(${HDRS})
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)

    list(APPEND ${SRCS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc")
    list(APPEND ${HDRS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h")

    add_custom_command(
      OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc"
             "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h"
      COMMAND  ${PROTOBUF3_PROTOC3_EXECUTABLE}
      ARGS --cpp_out  ${CMAKE_CURRENT_BINARY_DIR} ${_protobuf_include_path} ${ABS_FIL}
      DEPENDS ${ABS_FIL}
      COMMENT "Running C++ protocol buffer compiler on ${FIL}"
      VERBATIM ) 
  endforeach()

  set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
  set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Protobuf3 DEFAULT_MSG
  PROTOBUF3_INCLUDE_DIRS PROTOBUF3_LIBRARIES PROTOBUF3_RPATH)
