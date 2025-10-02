# SPDX-FileCopyrightText: 2019 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

find_package(Protobuf3 REQUIRED)
find_package(GRPC REQUIRED)
#
# XRootD SSI Protocol Buffer bindings
#
include_directories(${XRD_SSI_PB_DIR}/include ${XRD_SSI_PB_DIR}/eos_cta/include)

#
# Compiled protocol buffers
#
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})


add_library(EosGrpcClient STATIC GrpcClient.cpp GrpcUtils.cpp GrpcEndpoint.cpp)

target_link_libraries(EosGrpcClient ${PROTOBUF3_LIBRARIES} ${GRPC_LIBRARY} ${GRPC_GRPC++_LIBRARY})
set_property (TARGET EosGrpcClient APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
