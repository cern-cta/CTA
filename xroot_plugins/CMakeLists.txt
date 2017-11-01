# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 2.6)

list (APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package (xrootd REQUIRED)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR})
include_directories(${CMAKE_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

find_package(Protobuf3 REQUIRED)
include_directories(${PROTOBUF3_INCLUDE_DIRS})

# XRootD OFS plugin for CTA Frontend

add_library (XrdCtaOfs MODULE ListArchiveFilesCmd.cpp XrdCtaFilesystem.cpp XrdCtaFile.cpp XrdCtaDir.cpp)
set_property(TARGET XrdCtaOfs PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET XrdCtaOfs PROPERTY   VERSION "${CTA_LIBVERSION}")
target_link_libraries (XrdCtaOfs ctacatalogue ctascheduler ctacommon ctaobjectstore cryptopp)
set_property (TARGET XrdCtaOfs APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET XrdCtaOfs APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

# XRootD SSI plugin for CTA Frontend

add_subdirectory(messages)

include_directories(../xroot_ssi_pb)

add_library(XrdSsiCta MODULE XrdSsiCtaServiceProvider.cpp XrdSsiCtaRequestProc.cpp XrdSsiCtaRequestMessage.cpp ../cmdline/CtaAdminCmdParse.cpp)
target_link_libraries(XrdSsiCta XrdSsi-4 XrdSsiLib XrdCtaMessages ctascheduler ctacommon ctaobjectstore ctacatalogue)
set_property (TARGET XrdSsiCta APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET XrdSsiCta APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)
# Get some extra debug messages on stdout
#target_compile_definitions(XrdSsiCta PRIVATE XRDSSI_DEBUG)

install (TARGETS XrdCtaOfs DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install (TARGETS XrdSsiCta DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install (FILES xrootd-cta.cfg DESTINATION /etc/xrootd/)
install (FILES cta-frontend.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
install (FILES cta-frontend.logrotate DESTINATION /etc/logrotate.d RENAME cta-frontend)
install (FILES cta-frontend.service DESTINATION /etc/systemd/system)
