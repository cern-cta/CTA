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

find_package(Protobuf3 REQUIRED)

include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})


set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow")

add_custom_command (OUTPUT mysql_statistics_schema.sql
COMMAND cat 
    ${CMAKE_CURRENT_SOURCE_DIR}/common_statistics_schema.sql 
    | sed 's/UINT8TYPE/TINYINT UNSIGNED/g'
    | sed 's/UINT16TYPE/SMALLINT UNSIGNED/g'
    | sed 's/UINT32TYPE/INT UNSIGNED/g'
    | sed 's/UINT64TYPE/BIGINT UNSIGNED/g'
    | sed 's/CHECKSUM_BLOB_TYPE/VARBINARY\(200\)/g'
    | tee mysql_statistics_schema.sql > /dev/null
DEPENDS  ${CMAKE_CURRENT_SOURCE_DIR}/common_statistics_schema.sql
)

# For Mysql
add_custom_command(OUTPUT mysql_statistics_schema.cpp
  COMMAND sed 's/^/\ \ \"/' mysql_statistics_schema.sql | sed 's/$$/\"/' > mysql_statistics_schema.cpp
  DEPENDS mysql_statistics_schema.sql)

add_custom_command(OUTPUT MysqlStatisticsSchema.cpp
  COMMAND sed -e '/CTA_SQL_SCHEMA/r mysql_statistics_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/MysqlStatisticsSchema.before_SQL.cpp > MysqlStatisticsSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/MysqlStatisticsSchema.before_SQL.cpp mysql_statistics_schema.cpp)

set (STATISTICS_LIB_SRC_FILES
  StatisticsSchema.cpp
  MysqlStatisticsSchema.cpp
  StatisticsSchemaFactory.cpp
  TapeStatisticsUpdater.cpp
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
  MysqlStatisticsSchema.cpp
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