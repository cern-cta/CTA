# SPDX-FileCopyrightText: 2021 CERN
# SPDX-FileCopyrightText: 2021 DESY
# SPDX-License-Identifier: GPL-3.0-or-later


include_directories(${CMAKE_BINARY_DIR}/lib/protobuf ${PROTOBUF3_INCLUDE_DIRS})

# include stuff from the callback api here because we need to serve the streaming commands by registering their service class impl
add_executable(cta-frontend-grpc
  Main.cpp
  ${PROJECT_SOURCE_DIR}/tools/CtaAdminCmdParser.cpp
  FrontendGrpcService.cpp
  ${PROJECT_SOURCE_DIR}/frontend/grpc/common/GrpcAuthUtils.cpp
  ${PROJECT_SOURCE_DIR}/frontend/grpc/common/GrpcConfig.cpp
  callback_api/CtaAdminServerWriteReactor.cpp
  NegotiationService.cpp
  ServerNegotiationRequestHandler.cpp
  TokenStorage.cpp
  utils.cpp)
if(CTA_USE_PGSCHED)
  target_link_libraries(cta-frontend-grpc
    ${GRPC_GRPC++_REFLECTION_LIBRARY}
    ctafrontendcommon ctascheduler ctacommon ctacatalogue ctagrpc ctaauth gssapi_krb5)
else()
  target_link_libraries(cta-frontend-grpc
    ${GRPC_GRPC++_REFLECTION_LIBRARY}
    ctafrontendcommon ctascheduler ctacommon ctaobjectstore ctacatalogue ctagrpc ctaauth gssapi_krb5)
endif()

add_manpage(cta-frontend-grpc)

install (TARGETS cta-frontend-grpc DESTINATION usr/bin)
install (FILES cta-frontend-grpc.sysconfig DESTINATION /etc/sysconfig RENAME cta-frontend-grpc)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-frontend-grpc.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-frontend.example.conf cta-frontend-grpc.example.toml jwt-revoke-list.example.toml DESTINATION usr/${CMAKE_INSTALL_DOCDIR})

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
  ctacommon
  stdc++fs
  ctatapedaemoncommon
  ctagrpc
  gssapi_krb5)

set_property (TARGET cta-admin APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
install(TARGETS cta-admin DESTINATION usr/bin)
