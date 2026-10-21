# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})

add_compile_options(-Wshadow)

set (STATISTICS_LIB_SRC_FILES
  Statistics.cpp
  FileStatistics.cpp
  DatabaseStatisticsService.cpp
  DatabaseStatisticsServiceFactory.cpp
  StatisticsServiceFactory.cpp
  JsonStatisticsService.cpp
)

add_library (ctastatistics SHARED
   ${STATISTICS_LIB_SRC_FILES})

set_property(TARGET ctastatistics PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctastatistics PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctastatistics DESTINATION ${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctastatistics
  ctacatalogue
  ctacommon
  ctaschemachecker
  ctardbms)

add_executable(cta-statistics-save
  StatisticsSaveCmd.cpp
  StatisticsSaveCmdLineArgs.cpp
  StatisticsSaveCmdMain.cpp
)

target_link_libraries (cta-statistics-save ctastatistics)

install (TARGETS cta-statistics-save DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(cta-statistics-update
  StatisticsUpdateCmd.cpp
  StatisticsUpdateCmdLineArgs.cpp
  StatisticsUpdateCmdMain.cpp
)

target_link_libraries (cta-statistics-update ctastatistics)

install (TARGETS cta-statistics-update DESTINATION ${CMAKE_INSTALL_BINDIR})

add_manpage(cta-statistics-save)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-statistics-save.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)

add_manpage(cta-statistics-update)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-statistics-update.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
