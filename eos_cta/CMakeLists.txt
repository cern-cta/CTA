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
set(PROTOBUF_CTA_FRONTEND_DIR ${XRD_SSI_PB_DIR}/eos_cta/protobuf)
set(PROTOBUF_EOS_MGM_DIR ${PROJECT_SOURCE_DIR}/eos_cta/grpc-proto/protobuf)
file(GLOB ProtoFilesCtaFrontend "${PROTOBUF_CTA_FRONTEND_DIR}/*.proto")
file(GLOB ProtoFilesEOSMGM "${PROTOBUF_EOS_MGM_DIR}/*.proto")

# Compile protobufs
set(PROTOBUF3_INCLUDE_PATH ${PROTOBUF_CTA_FRONTEND_DIR}:${PROTOBUF_EOS_MGM_DIR})
PROTOBUF3_GENERATE_CPP(ProtoSourcesCtaFrontend ProtoHeadersCtaFrontend ${ProtoFilesCtaFrontend})
PROTOBUF3_GENERATE_CPP(ProtoSourcesEOSMGM ProtoHeadersEOSMGM ${ProtoFilesEOSMGM})

# Compile gRPC code
set(GRPC_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/eos_cta/")
grpc_generate_cpp(GrpcSourcesEOSMGM GrpcHeadersEOSMGM ${GRPC_PROTOBUF_PATH} ${ProtoFilesEOSMGM})
set(PROTOBUF3_IMPORT_DIRS "${PROTOBUF_CTA_FRONTEND_DIR}:${PROTOBUF3_INCLUDE_DIRS}")
grpc_generate_cpp(GrpcSourcesCtaFrontend GrpcHeadersCtaFrontend ${GRPC_PROTOBUF_PATH} ${ProtoFilesCtaFrontend})

set_source_files_properties(
  ${ProtoSourcesCtaFrontend} ${ProtoHeadersCtaFrontend}
  ${ProtoSourcesEOSMGM} ${ProtoHeadersEOSMGM}
  ${GrpcSourcesEOSMGM} ${GrpcHeadersEOSMGM}
  ${GrpcSourcesCtaFrontend} ${GrpcHeadersCtaFrontend}
  PROPERTIES GENERATED TRUE)

add_library(ProtoCtaFrontend ${ProtoSourcesCtaFrontend} ${GrpcSourcesCtaFrontend})
set_target_properties(ProtoCtaFrontend PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_link_libraries(ProtoCtaFrontend gpr absl_synchronization ${PROTOBUF3_LIBRARIES})

# Library containing EOS protobuf and gRPC definitions
add_library(ProtoEOSMGM ${ProtoSourcesEOSMGM} ${GrpcSourcesEOSMGM})
set_target_properties(ProtoEOSMGM PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_link_libraries(ProtoEOSMGM gpr absl_synchronization ${PROTOBUF3_GRPC_LIBRARIES})
