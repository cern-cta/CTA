# SPDX-FileCopyrightText: 2009 Kitware, Inc.
# SPDX-FileCopyrightText: 2009-2011 Philip Lowman <philip@yhbt.com>
# SPDX-FileCopyrightText: 2008 Esben Mose Hansen, Ange Optimization ApS
# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: BSD-3-Clause

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

set(PROTOBUF3_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR})

# Use the same prefix search policy for the compiler, headers, and library.
find_program(PROTOBUF3_PROTOC3_EXECUTABLE
  NAMES protoc
  DOC "The Google Protocol Buffers Compiler")
find_path(PROTOBUF3_INCLUDE_DIRS
  NAMES google/protobuf/message.h)
find_library(PROTOBUF3_LIBRARIES
  NAMES protobuf)

get_filename_component(PROTOBUF3_RPATH "${PROTOBUF3_LIBRARIES}" DIRECTORY)

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
  PROTOBUF3_INCLUDE_DIRS PROTOBUF3_LIBRARIES PROTOBUF3_PROTOC3_EXECUTABLE)
