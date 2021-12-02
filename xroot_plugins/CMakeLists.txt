# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2018-2021 CERN
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

find_package(xrootd REQUIRED)
find_package(Protobuf3 REQUIRED)

include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

#
# XRootD SSI Protocol Buffer bindings
#
include_directories(${XRD_SSI_PB_DIR}/include ${XRD_SSI_PB_DIR}/eos_cta/include)

#
# Compiled protocol buffers
#
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

#
# XRootD SSI plugin for CTA Frontend
#
add_library(XrdSsiCta MODULE XrdSsiCtaServiceProvider.cpp XrdSsiCtaRequestProc.cpp XrdSsiCtaRequestMessage.cpp
                             ../cmdline/CtaAdminCmdParse.cpp
                             GrpcClient.cpp GrpcEndpoint.cpp)
target_link_libraries(XrdSsiCta ${XROOTD_XRDSSI_LIB} XrdSsiLib XrdSsiPbEosCta ctascheduler ctacommon ctaobjectstore ctacatalogue
                      EosMigration ${GRPC_LIBRARY} ${GRPC_GRPC++_LIBRARY})
set_property (TARGET XrdSsiCta APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET XrdSsiCta APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

install(TARGETS XrdSsiCta DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(FILES cta-frontend-xrootd.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
install(FILES cta-frontend.logrotate DESTINATION /etc/logrotate.d RENAME cta-frontend)
install(FILES cta-frontend.sysconfig DESTINATION /etc/sysconfig RENAME cta-frontend)
install(FILES cta-frontend.service DESTINATION /etc/systemd/system)
