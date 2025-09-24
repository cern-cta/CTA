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

include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

include_directories(${CMAKE_SOURCE_DIR}/common/jwt-cpp/include/)

include_directories(${CMAKE_SOURCE_DIR}/common/jwt-cpp/include/)

# include stuff from the callback api here because we need to serve the streaming commands by registering their service class impl
add_executable(cta-frontend-grpc Main.cpp FrontendGrpcService.cpp
  ../common/WorkflowEvent.cpp
  ../common/FrontendService.cpp
  ../common/ValidateToken.cpp
  ../common/AdminCmd.cpp
  ../common/AdminCmdOptions.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminTextFormatter.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminCmdParser.cpp
  ${PROJECT_SOURCE_DIR}/common/config/Config.cpp
  callback_api/CtaAdminServerWriteReactor.cpp) # This is needed for the Impl Class that serves the streaming commands
# target_link_libraries(cta-frontend-grpc ProtoCtaFrontend)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-frontend-grpc
    ${GRPC_GRPC++_REFLECTION_LIBRARY}
    ctascheduler ctacommon ctacatalogue ctagrpc JwkCache)
else()
  target_link_libraries(cta-frontend-grpc
    ${GRPC_GRPC++_REFLECTION_LIBRARY}
    ctascheduler ctacommon ctaobjectstore ctacatalogue ctagrpc JwkCache)
endif()

add_manpage(cta-frontend-grpc)

install (TARGETS cta-frontend-grpc DESTINATION usr/bin)
install (FILES cta-frontend-grpc.service DESTINATION etc/systemd/system)
install (FILES cta-frontend-grpc.sysconfig DESTINATION /etc/sysconfig RENAME cta-frontend-grpc)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-frontend-grpc.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-frontend-grpc.conf.example DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)

##
## cta-admin-grpc
##
include_directories(${XRD_SSI_PB_DIR}/eos_cta/include)

# Compiled protocol buffers
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

add_executable(cta-admin-grpc
  CtaAdminGrpcCmd.cpp
  KeycloakClient.cpp
  ${PROJECT_SOURCE_DIR}/frontend/common/AdminCmd.cpp
  ${PROJECT_SOURCE_DIR}/frontend/common/AdminCmdOptions.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminParsedCmd.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminTextFormatter.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminCmdParser.cpp
)

target_link_libraries(cta-admin-grpc
  ctacommon stdc++fs ctatapedaemoncommon ctascheduler ctaobjectstore ctacatalogue ctagrpc curl
)
# if(CTA_USE_PGSCHED)
#   target_link_libraries(cta-admin-grpc ctaobjectstore)
# endif()
set_property (TARGET cta-admin-grpc APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
install(TARGETS cta-admin-grpc DESTINATION usr/bin)

##
## cta-frontend-grpc-stream-server
##

add_executable(cta-frontend-async-grpc
  FrontendCmd.cpp
  AsyncServer.cpp
  TokenStorage.cpp
  ServiceKerberosAuthProcessor.cpp
  ServerTapeLsRequestHandler.cpp
  ServerNegotiationRequestHandler.cpp
  RequestMessage.cpp
  ${PROJECT_SOURCE_DIR}/frontend/common/AdminCmdOptions.cpp
  utils.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminCmdParser.cpp
)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-frontend-async-grpc
          ctascheduler ctacommon ctatapedaemoncommon ctacatalogue gssapi_krb5 ctagrpc)
else()
  target_link_libraries(cta-frontend-async-grpc
          ctascheduler ctacommon ctatapedaemoncommon ctaobjectstore ctacatalogue gssapi_krb5 ctagrpc)
endif()
