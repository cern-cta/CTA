# SPDX-FileCopyrightText: 2015 CERN
# SPDX-FileCopyrightText: 2021 CERN
# SPDX-FileCopyrightText: 2021 DESY
# SPDX-License-Identifier: GPL-3.0-or-later


# Compiled protocol buffers
include_directories(${CMAKE_BINARY_DIR}/lib/protobuf ${PROTOBUF3_INCLUDE_DIRS})

add_executable(cta-admin
  CtaAdminGrpcCmd.cpp
  CtaAdminParsedCmd.cpp
  CtaAdminTextFormatter.cpp
  ClientNegotiationRequestHandler.cpp
  utils.cpp
)

target_link_libraries(cta-admin
  cta-admin-api-cmd-parser
  cta-grpc-server-config
  ctacommon
  stdc++fs
  ctagrpc
  ctaprotobufconversions
  ctatapedutils
  gssapi_krb5)

# Only add a runtime search path for a non-system Protobuf installation.
if(NOT PROTOBUF3_RPATH IN_LIST CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES)
  set_property(TARGET cta-admin APPEND PROPERTY INSTALL_RPATH "${PROTOBUF3_RPATH}")
endif()
install(TARGETS cta-admin DESTINATION ${CMAKE_INSTALL_BINDIR})

set(CTA_ADMIN_MANPAGE_MARKDOWN "${CMAKE_CURRENT_BINARY_DIR}/cta-admin.1cta.md")
add_custom_command(
  OUTPUT "${CTA_ADMIN_MANPAGE_MARKDOWN}"
  COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/ci/build/compile_man_md.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/cta-admin.1cta.md.in"
    "${CTA_ADMIN_MANPAGE_MARKDOWN}"
  DEPENDS
    "${PROJECT_SOURCE_DIR}/ci/build/compile_man_md.py"
    cta-admin.1cta.md.in
    "${PROJECT_SOURCE_DIR}/app/cta-admin-api/src/commands/CtaAdminCmdParser.hpp"
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Generating cta-admin man-page Markdown"
  VERBATIM
)
add_manpage(cta-admin SOURCE "${CTA_ADMIN_MANPAGE_MARKDOWN}")

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-admin.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install(FILES cta-admin.example.conf DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/doc/cta-admin/examples)
