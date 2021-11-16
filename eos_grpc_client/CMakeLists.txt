# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2019-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 2.6)

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

