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

add_executable(cta-frontend-grpc
  Main.cpp
  FrontendGrpcService.cpp
  ../common/WorkflowEvent.cpp
  ../common/FrontendService.cpp
  ../common/ValidateToken.cpp)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-frontend-grpc
    ctascheduler ctacommon ctacatalogue ctagrpc JwkCache)
else()
  target_link_libraries(cta-frontend-grpc
    ctascheduler ctacommon ctaobjectstore ctacatalogue ctagrpc JwkCache)
endif()

add_manpage(cta-frontend-grpc)

install (TARGETS cta-frontend-grpc DESTINATION usr/bin)
install (FILES cta-frontend-grpc.service DESTINATION etc/systemd/system)
install (FILES cta-frontend-grpc.sysconfig DESTINATION /etc/sysconfig RENAME cta-frontend-grpc)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-frontend-grpc.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-frontend-grpc.conf.example DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)

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

##
## cta-frontend-grpc-stream-client
##

add_executable(cta-admin-grpc
  CtaAdminGrpcCmd.cpp
  ClientTapeLsRequestHandler.cpp
  ClientNegotiationRequestHandler.cpp
  utils.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminTextFormatter.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminCmdParser.cpp
)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-admin-grpc
          ctascheduler ctacommon ctatapedaemoncommon ctacatalogue gssapi_krb5 ctagrpc)
else()
  target_link_libraries(cta-admin-grpc
          ctascheduler ctacommon ctatapedaemoncommon ctaobjectstore ctacatalogue gssapi_krb5 ctagrpc)
endif()
