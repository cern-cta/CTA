# SPDX-FileCopyrightText: 2021 CERN
# SPDX-FileCopyrightText: 2021 DESY
# SPDX-License-Identifier: GPL-3.0-or-later


include_directories(${CMAKE_BINARY_DIR}/lib/protobuf ${PROTOBUF3_INCLUDE_DIRS})

##
## cta-admin
##
# Compiled protocol buffers
include_directories(${CMAKE_BINARY_DIR}/lib/protobuf ${PROTOBUF3_INCLUDE_DIRS})

add_executable(cta-admin
  CtaAdminGrpcCmd.cpp
  ${PROJECT_SOURCE_DIR}/tools/CtaAdminParsedCmd.cpp
  ${PROJECT_SOURCE_DIR}/tools/CtaAdminTextFormatter.cpp
  ${PROJECT_SOURCE_DIR}/tools/CtaAdminCmdParser.cpp
  ClientNegotiationRequestHandler.cpp
  utils.cpp
)

target_link_libraries(cta-admin
  cta-grpc-server-config
  ctacommon
  stdc++fs
  ctatapedaemoncommon
  ctagrpc
  ctaprotobufconversions
  gssapi_krb5)

# Only add a runtime search path for a non-system Protobuf installation.
if(NOT PROTOBUF3_RPATH IN_LIST CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES)
  set_property(TARGET cta-admin APPEND PROPERTY INSTALL_RPATH "${PROTOBUF3_RPATH}")
endif()
install(TARGETS cta-admin DESTINATION ${CMAKE_INSTALL_BINDIR})
