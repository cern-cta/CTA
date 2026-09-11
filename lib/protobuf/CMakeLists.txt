# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

include_directories(${PROTOBUF3_INCLUDE_DIRS})

# Select protobuf files
set(PROTOBUF_CTA_FRONTEND_DIR ${XRD_SSI_PB_DIR}/eos_cta/protobuf)
file(GLOB ProtoFilesCtaFrontend "${PROTOBUF_CTA_FRONTEND_DIR}/*.proto")

# Compile protobufs
set(PROTOBUF3_INCLUDE_PATH ${PROTOBUF_CTA_FRONTEND_DIR})
PROTOBUF3_GENERATE_CPP(ProtoSourcesCtaFrontend ProtoHeadersCtaFrontend ${ProtoFilesCtaFrontend})

# Compile gRPC code
set(GRPC_PROTOBUF_PATH "${CMAKE_CURRENT_BINARY_DIR}/")
set(PROTOBUF3_IMPORT_DIRS "${PROTOBUF_CTA_FRONTEND_DIR}:${PROTOBUF3_INCLUDE_DIRS}")
grpc_generate_cpp(GrpcSourcesCtaFrontend GrpcHeadersCtaFrontend ${GRPC_PROTOBUF_PATH} ${ProtoFilesCtaFrontend})

set_source_files_properties(
  ${ProtoSourcesCtaFrontend} ${ProtoHeadersCtaFrontend}
  ${GrpcSourcesCtaFrontend} ${GrpcHeadersCtaFrontend}
  PROPERTIES GENERATED TRUE)

# Generated CTA protobuf message types shared by clients and servers.
add_library(ctaprotobuf ${ProtoSourcesCtaFrontend})
set_target_properties(ctaprotobuf PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_include_directories(ctaprotobuf PUBLIC
  ${PROJECT_SOURCE_DIR}/lib
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROTOBUF3_INCLUDE_DIRS})
target_link_libraries(ctaprotobuf ${PROTOBUF3_LIBRARIES})

# Domain-to-protobuf conversions, kept separate so ctacommon remains protobuf-free.
add_library(ctaprotobufconversions
  src/common/conversions/ChecksumBlobSerDeser.cpp)
set_target_properties(ctaprotobufconversions PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_include_directories(ctaprotobufconversions PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(ctaprotobufconversions
  ctaprotobuf
  ctacommon)

add_library(ctagrpc ${GrpcSourcesCtaFrontend})
set_target_properties(ctagrpc PROPERTIES
  LINKER_LANGUAGE CXX
  POSITION_INDEPENDENT_CODE TRUE)
target_link_libraries(ctagrpc
  ctaprotobuf
  gpr
  absl_synchronization
  ${PROTOBUF3_GRPC_LIBRARIES}
  ${GRPC_GRPC++_LIBRARY})

add_library(ctaprotobufunittests SHARED
  test/common/conversions/ChecksumBlobSerDeserTest.cpp)
set_property(TARGET ctaprotobufunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaprotobufunittests PROPERTY VERSION "${CTA_LIBVERSION}")
target_link_libraries(ctaprotobufunittests ctaprotobufconversions)

install(TARGETS ctaprotobufunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
