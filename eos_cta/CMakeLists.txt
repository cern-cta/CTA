# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.
cmake_minimum_required(VERSION 3.17)

find_package(Protobuf3 REQUIRED)
find_package(GRPC REQUIRED)

include_directories(${PROTOBUF3_INCLUDE_DIRS})

# Select protobuf files
set(PROTOBUF_EOS_CTA_DIR ${XRD_SSI_PB_DIR}/eos_cta/protobuf)
file(GLOB ProtoFilesEosCta "${PROTOBUF_EOS_CTA_DIR}/*.proto")
set(PROTOBUF_MIGRATION_DIR ${PROJECT_SOURCE_DIR}/migration/grpc-proto/protobuf)
file(GLOB ProtoFilesMigration "${PROTOBUF_MIGRATION_DIR}/*.proto")

# Compile protobufs
set(PROTOBUF3_INCLUDE_PATH ${PROTOBUF_EOS_CTA_DIR}:${PROTOBUF_MIGRATION_DIR})
PROTOBUF3_GENERATE_CPP(ProtoSourcesEosCta ProtoHeadersEosCta ${ProtoFilesEosCta})
PROTOBUF3_GENERATE_CPP(ProtoSourcesMigration ProtoHeadersMigration ${ProtoFilesMigration})

# Compile gRPC code
set(GRPC_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/eos_cta/")
grpc_generate_cpp(ProtoGrpcSourcesMigration ProtoGrpcHeadersMigration ${GRPC_PROTOBUF_PATH} ${ProtoFilesMigration})

set_source_files_properties(
  ${ProtoSourcesEosCta} ${ProtoHeadersEosCta}
  ${ProtoSourcesMigration} ${ProtoHeadersMigration}
  ${ProtoGrpcSourcesMigration} ${ProtoGrpcHeadersMigration}
  PROPERTIES GENERATED TRUE)

foreach(PROTO_SRC ${ProtoSourcesEosCta} ${ProtoSourcesMigration} ${ProtoGrpcSourcesMigration})
  set_property(SOURCE ${PROTO_SRC} PROPERTY COMPILE_FLAGS " -Wno-missing-field-initializers -fPIC -Wno-narrowing -Wno-implicit-fallthrough")
  # Add -Wno-narrowing -Wno-implicit-fallthrough compiler flags if using gcc version 7 or greater
  if(CMAKE_COMPILER_IS_GNUCC)
    if(GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
      set_property(SOURCE ${PROTO_SRC} APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-narrowing -Wno-implicit-fallthrough")
    endif(GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
  endif(CMAKE_COMPILER_IS_GNUCC)
endforeach(PROTO_SRC)

add_library(XrdSsiPbEosCta ${ProtoSourcesEosCta})
set_target_properties(XrdSsiPbEosCta PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_link_libraries(XrdSsiPbEosCta ${PROTOBUF3_LIBRARIES})

add_library(EosMigration ${ProtoSourcesMigration} ${ProtoGrpcSourcesMigration})
set_target_properties(EosMigration PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_link_libraries(EosMigration ${PROTOBUF3_GRPC_LIBRARIES})
