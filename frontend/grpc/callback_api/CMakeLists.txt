# SPDX-FileCopyrightText: 2021 CERN
# SPDX-FileCopyrightText: 2021 DESY
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

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
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminCmdParser.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminParsedCmd.cpp
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

install(TARGETS cta-admin-grpc-stream DESTINATION usr/bin)
