# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow")

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

install (TARGETS ctastatistics DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctastatistics
  ctacatalogue
  ctacommon
  ctaschemachecker
  ctardbms)

install (FILES ${CMAKE_SOURCE_DIR}/catalogue/cta-catalogue.conf.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

add_executable(cta-statistics-save
  StatisticsSaveCmd.cpp
  StatisticsSaveCmdLineArgs.cpp
  StatisticsSaveCmdMain.cpp
)

target_link_libraries (cta-statistics-save ctastatistics)

install (TARGETS cta-statistics-save DESTINATION /usr/bin)

add_executable(cta-statistics-update
  StatisticsUpdateCmd.cpp
  StatisticsUpdateCmdLineArgs.cpp
  StatisticsUpdateCmdMain.cpp
)

target_link_libraries (cta-statistics-update ctastatistics)

install (TARGETS cta-statistics-update DESTINATION /usr/bin)
