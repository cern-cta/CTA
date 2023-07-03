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

set(PROTOBUF_CtaFrontend_DIR ${PROJECT_SOURCE_DIR}/frontend-grpc/grpc-proto/protobuf)
file(GLOB ProtoFilesCtaFrontend "${PROTOBUF_CtaFrontend_DIR}/*.proto")

# depends on CTA_EOS definitions
set(PROTOBUF_EOS_CTA_DIR ${XRD_SSI_PB_DIR}/eos_cta/protobuf)
file(GLOB ProtoFilesEosCta "${PROTOBUF_EOS_CTA_DIR}/*.proto")

set(PROTOBUF3_INCLUDE_PATH "${PROTOBUF_EOS_CTA_DIR}:${PROTOBUF_CtaFrontend_DIR}:${PROTOBUF3_INCLUDE_DIRS}")
PROTOBUF3_GENERATE_CPP(ProtoSourcesCtaFrontend ProtoHeadersCtaFrontend ${ProtoFilesCtaFrontend})

set(GRPC_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/frontend-grpc")
set(PROTOBUF3_IMPORT_DIRS "${PROTOBUF_EOS_CTA_DIR}:${PROTOBUF3_INCLUDE_DIRS}")
grpc_generate_cpp(ProtoGrpcSourcesCtaFrontend ProtoGrpcHeadersCtaFrontend ${GRPC_PROTOBUF_PATH} ${ProtoFilesCtaFrontend})

set_source_files_properties(
  ${ProtoSourcesCtaFrontend} ${ProtoHeadersCtaFrontend}
  ${ProtoGrpcSourcesCtaFrontend} ${ProtoGrpcHeadersCtaFrontend}
  PROPERTIES GENERATED TRUE
)

foreach(PROTO_SRC ${ProtoSourcesCtaFrontend} ${ProtoGrpcSourcesCtaFrontend})
  set_property(SOURCE ${PROTO_SRC} PROPERTY COMPILE_FLAGS " -Wno-missing-field-initializers -fPIC -Wno-narrowing -Wno-implicit-fallthrough")
  # Add -Wno-narrowing -Wno-implicit-fallthrough compiler flags if using gcc version 7 or greater
  if(CMAKE_COMPILER_IS_GNUCC)
    if(GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
      set_property(SOURCE ${PROTO_SRC} APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-narrowing -Wno-implicit-fallthrough")
    endif(GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
  endif(CMAKE_COMPILER_IS_GNUCC)
endforeach(PROTO_SRC)

include_directories(${CMAKE_BINARY_DIR}/frontend-grpc ${CMAKE_BINARY_DIR}/eos_cta
                    ${PROTOBUF3_INCLUDE_DIRS})

add_executable(cta-frontend-grpc Main.cpp FrontendGRpcSvc.cpp ${ProtoSourcesCtaFrontend} ${ProtoGrpcSourcesCtaFrontend})

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-frontend-grpc ${PROTOBUF3_LIBRARIES} ${GRPC_GRPC++_LIBRARY}
    ctascheduler ctacommon ctacatalogue)
else()
  target_link_libraries(cta-frontend-grpc ${PROTOBUF3_LIBRARIES} ${GRPC_GRPC++_LIBRARY}
    ctascheduler ctacommon ctaobjectstore ctacatalogue)
endif()
set_property(TARGET cta-frontend-grpc APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

install(TARGETS cta-frontend-grpc DESTINATION usr/bin)
install (FILES cta-frontend-grpc.service DESTINATION etc/systemd/system)
install (FILES cta-frontend-grpc.sysconfig DESTINATION /etc/sysconfig RENAME cta-frontend-grpc)
install (FILES cta-frontend-grpc.1cta DESTINATION /usr/share/man/man1)

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
  ${PROJECT_SOURCE_DIR}/cmdline/CtaAdminCmdParse.cpp ${ProtoSourcesCtaFrontend} ${ProtoGrpcSourcesCtaFrontend}
)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-frontend-async-grpc ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY} ${GRPC_LIBRARY}
          ctascheduler ctacommon ctacatalogue gssapi_krb5)
else()
  target_link_libraries(cta-frontend-async-grpc ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY} ${GRPC_LIBRARY}
          ctascheduler ctacommon ctaobjectstore ctacatalogue gssapi_krb5)
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
  ${ProtoSourcesCtaFrontend}
  ${ProtoGrpcSourcesCtaFrontend}
)

if(CTA_USE_PGSCHED)
  target_link_libraries(cta-admin-grpc
          ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY}
          ${GRPC_LIBRARY}
          ctascheduler ctacommon ctacatalogue cryptopp gssapi_krb5)
else()
  target_link_libraries(cta-admin-grpc
          ${PROTOBUF3_LIBRARIES}
          ${GRPC_GRPC++_LIBRARY}
          ${GRPC_LIBRARY}
          ctascheduler ctacommon ctaobjectstore ctacatalogue cryptopp gssapi_krb5)
endif()