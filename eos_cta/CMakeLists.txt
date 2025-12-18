# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
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

add_library(ctaprotobuf ${ProtoSourcesCtaFrontend})
set_target_properties(ctaprotobuf PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_link_libraries(ctaprotobuf ${PROTOBUF3_LIBRARIES})

add_library(ctagrpc ${GrpcSourcesCtaFrontend})
set_target_properties(ctagrpc PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_link_libraries(ctagrpc gpr absl_synchronization ${PROTOBUF3_GRPC_LIBRARIES} ${GRPC_GRPC++_LIBRARY})

add_dependencies(ctagrpc ctaprotobuf)

# Library containing EOS protobuf and gRPC definitions
add_library(ProtoEOSMGM ${ProtoSourcesEOSMGM} ${GrpcSourcesEOSMGM})
set_target_properties(ProtoEOSMGM PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_link_libraries(ProtoEOSMGM gpr absl_synchronization ${PROTOBUF3_GRPC_LIBRARIES} ${GRPC_GRPC++_LIBRARY})
