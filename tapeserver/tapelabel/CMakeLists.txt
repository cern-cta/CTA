# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.
cmake_minimum_required (VERSION 3.17)

add_document(
  TARGET cta-tape-label_man_page
  OUTPUT_FILE cta-tape-label.1cta
  SOURCES cta-tape-label.1cta.md
  NO_EXPORT_PRODUCT
  PANDOC_DIRECTIVES --standalone --to man
)

add_executable(cta-tape-label
  TapeLabelCmd.cpp
  TapeLabelCmdLineArgs.cpp
  TapeLabelCmdMain.cpp)

target_link_libraries (cta-tape-label
  ctacommon
  TapeDrive
  ctamediachanger
  ctacatalogue
  SCSI
)

add_library(ctatapelabelunittests SHARED
  TapeLabelCmdLineArgs.cpp
  TapeLabelCmdLineArgsTest.cpp)

set_property(TARGET ctatapelabelunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapelabelunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS cta-tape-label DESTINATION /usr/bin)
install(TARGETS ctatapelabelunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-tape-label.1cta DESTINATION /usr/share/man/man1)
