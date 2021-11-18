# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
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
cmake_minimum_required (VERSION 2.6)

find_package(Protobuf3 REQUIRED)

include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow")

set (STATISTICS_LIB_SRC_FILES
  Statistics.cpp
  FileStatistics.cpp
  StatisticsService.cpp
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

set_property(TARGET cta-statistics-save APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
set_property(TARGET cta-statistics-save APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})

install (TARGETS cta-statistics-save DESTINATION /usr/bin)

add_executable(cta-statistics-update
  StatisticsUpdateCmd.cpp
  StatisticsUpdateCmdLineArgs.cpp
  StatisticsUpdateCmdMain.cpp
)

target_link_libraries (cta-statistics-update ctastatistics)

set_property(TARGET cta-statistics-update APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
set_property(TARGET cta-statistics-update APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})

install (TARGETS cta-statistics-update DESTINATION /usr/bin)
