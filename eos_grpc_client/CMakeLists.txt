# SPDX-FileCopyrightText: 2019 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


add_library(EosGrpcClient STATIC GrpcClient.cpp GrpcUtils.cpp GrpcEndpoint.cpp)

target_include_directories(EosGrpcClient PRIVATE
  ${XRD_SSI_PB_DIR}/include
  ${XRD_SSI_PB_DIR}/eos_cta/include
  ${CMAKE_BINARY_DIR}/eos_cta
  ${PROTOBUF3_INCLUDE_DIRS}
)

target_link_libraries(EosGrpcClient ${PROTOBUF3_LIBRARIES} ${GRPC_LIBRARY} ${GRPC_GRPC++_LIBRARY})
set_property (TARGET EosGrpcClient APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
