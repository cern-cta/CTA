# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

find_package(xrootdclient REQUIRED)
find_package(Protobuf3 REQUIRED)

# XRootD SSI
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_INCLUDE_DIR}/private)

# XRootD SSI Protocol Buffer bindings
include_directories(${XRD_SSI_PB_DIR}/include ${XRD_SSI_PB_DIR}/eos_cta/include)

# Compiled protocol buffers
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

set(CMD_LINE_TOOLS_COMMON_LIB_SRC_FILES
  common/CatalogueFetch.cpp
  common/CmdLineArgs.cpp
  common/CmdLineTool.cpp
  common/ConnectionConfiguration.cpp
)

add_library (ctaCmdlineToolsCommon STATIC
  ${CMD_LINE_TOOLS_COMMON_LIB_SRC_FILES})

target_link_libraries(ctaCmdlineToolsCommon ctacommon XrdSsiLib XrdUtils ProtoEOSMGM EosGrpcClient)

set_property(TARGET ctaCmdlineToolsCommon PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaCmdlineToolsCommon PROPERTY   VERSION "${CTA_LIBVERSION}")

#
# cta-send-event CLOSEW|PREPARE|ABORT_PREPARE
# injects a workflow event into the CTA Frontend
#
add_executable(cta-send-event CtaSendEvent.cpp common/CmdLineArgs.cpp)
target_link_libraries(cta-send-event ctaCmdlineToolsCommon)
set_property(TARGET cta-send-event APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

#
# verify-file <archiveFileId> <vid>
# recalls a file from tape without writing a disk copy
#
add_executable(cta-verify-file CtaVerifyFile.cpp common/CmdLineArgs.cpp common/CatalogueFetch.cpp)
target_link_libraries(cta-verify-file ctacommon XrdSsiLib XrdUtils)
set_property (TARGET cta-verify-file APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

install(TARGETS cta-send-event DESTINATION usr/bin)
install(TARGETS cta-verify-file DESTINATION usr/bin)

add_subdirectory (change_storage_class)
add_subdirectory (restore_files)
add_subdirectory (eos_namespace_injection)
