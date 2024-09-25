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

add_executable(cta-frontend-grpc Main.cpp FrontendGRpcSvc.cpp
  ../common/WorkflowEvent.cpp
  ${PROJECT_SOURCE_DIR}/frontend/common/FrontendService.cpp
  ${PROJECT_SOURCE_DIR}/frontend/common/Config.cpp)
target_link_libraries(cta-frontend-grpc ProtoCtaFrontend)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-frontend-grpc ${PROTOBUF3_LIBRARIES} ${GRPC_GRPC++_LIBRARY}
    ctascheduler ctacommon ctacatalogue)
else()
  target_link_libraries(cta-frontend-grpc ${PROTOBUF3_LIBRARIES} ${GRPC_GRPC++_LIBRARY}
    ctascheduler ctacommon ctaobjectstore ctacatalogue)
endif()
set_property(TARGET cta-frontend-grpc APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

add_manpage(cta-frontend-grpc)

install(TARGETS cta-frontend-grpc DESTINATION usr/bin)
install (FILES cta-frontend-grpc.service DESTINATION etc/systemd/system)
install (FILES cta-frontend-grpc.sysconfig DESTINATION /etc/sysconfig RENAME cta-frontend-grpc)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-frontend-grpc.1cta DESTINATION /usr/share/man/man1)

##
## cta-frontend-grpc-stream-server
##

# XRootD SSI Protocol Buffer bindings
# CtaFrontendApi.hpp
include_directories(${XRD_SSI_PB_DIR}/include ${XRD_SSI_PB_DIR}/eos_cta/include)
# XRootD SSI
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_INCLUDE_DIR}/private)

add_executable(cta-frontend-async-grpc
  FrontendCmd.cpp
  AsyncServer.cpp
  TokenStorage.cpp
  ServiceAuthProcessor.cpp
  ServerTapeLsRequestHandler.cpp
  ServerNegotiationRequestHandler.cpp
  RequestMessage.cpp
  utils.cpp
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminCmdParse.cpp
)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-frontend-async-grpc ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY} ${GRPC_LIBRARY}
          ProtoCtaFrontend ctascheduler ctacommon ctatapedaemoncommon ctacatalogue gssapi_krb5)
else()
  target_link_libraries(cta-frontend-async-grpc ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY} ${GRPC_LIBRARY}
          ProtoCtaFrontend ctascheduler ctacommon ctatapedaemoncommon ctaobjectstore ctacatalogue gssapi_krb5)
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
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminCmdParse.cpp
)
target_link_libraries(cta-admin-grpc ProtoCtaFrontend)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-admin-grpc
          ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY}
          ${GRPC_LIBRARY}
          ctascheduler ctacommon ctatapedaemoncommon ctacatalogue cryptopp gssapi_krb5)
else()
  target_link_libraries(cta-admin-grpc
          ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY}
          ${GRPC_LIBRARY}
          ctascheduler ctacommon ctatapedaemoncommon ctaobjectstore ctacatalogue cryptopp gssapi_krb5)
endif()
