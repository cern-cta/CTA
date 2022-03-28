# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2019-2022 CERN
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
