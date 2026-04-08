# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

add_executable(cta-readtp
  ReadtpCmd.cpp
  ReadtpCmdMain.cpp
  ReadtpCmdLineArgs.cpp
  TapeFseqRange.cpp
  TapeFseqRangeSequence.cpp
  TapeFseqRangeListSequence.cpp
  TapeFseqSequenceParser.cpp)

target_link_libraries(cta-readtp
  ctacommon
  TapeDrive
  ctamediachanger
  ctacatalogue
  SCSI
)

add_manpage(cta-readtp)

install(TARGETS cta-readtp DESTINATION /usr/bin)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-readtp.1cta DESTINATION /usr/share/man/man1)
