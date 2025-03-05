# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 DESY
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

cmake_minimum_required (VERSION 3.17)

find_package(Protobuf3 REQUIRED)
find_package(GRPC REQUIRED)

include_directories(${PROTOBUF3_INCLUDE_DIRS})

# depends on CTA_EOS definitions
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

##
## cta-frontend-grpc-stream-server
##

# XRootD SSI Protocol Buffer bindings
# CtaFrontendApi.hpp
include_directories(${XRD_SSI_PB_DIR}/include ${XRD_SSI_PB_DIR}/eos_cta/include)
# XRootD SSI
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_INCLUDE_DIR}/private)

##
## cta-frontend-grpc-stream
## This is just the client code
##

add_executable(cta-admin-grpc-stream
  CtaAdminCmdStreaming.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminTextFormatter.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminCmdParse.cpp
)
target_link_libraries(cta-admin-grpc-stream ProtoCtaFrontend)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-admin-grpc-stream
          ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY}
          ${GRPC_LIBRARY}
          ctascheduler ctacommon ctatapedaemoncommon ctacatalogue cryptopp)
else()
  target_link_libraries(cta-admin-grpc-stream
          ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY}
          ${GRPC_LIBRARY}
          ctascheduler ctacommon ctatapedaemoncommon ctaobjectstore ctacatalogue cryptopp)
endif()
