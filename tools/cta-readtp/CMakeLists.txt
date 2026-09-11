# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

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
  ctatapeddrive
  ctamediachanger
  ctacatalogue
  ctascsi
)

add_manpage(cta-readtp)

install(TARGETS cta-readtp DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-readtp.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
