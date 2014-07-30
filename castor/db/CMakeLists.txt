#
#                      castor/db/CMakeLists.txt
#
# This file is part of the Castor project.
# See http://castor.web.cern.ch/castor
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# Steven.Murray@cern.ch
#
cmake_minimum_required (VERSION 2.6)

add_subdirectory (cnv)
if (${COMPILE_SERVER} STREQUAL "1")
  add_subdirectory (ora)
endif (${COMPILE_SERVER} STREQUAL "1")

# Generate the SQL schema creation script for the stager
FILE(READ stager_oracle_create.list STAGER_SQL_LIST)
STRING(REPLACE "\n" ";" STAGER_SQL_LIST "${STAGER_SQL_LIST}")
STRING(REPLACE " " "" STAGER_SQL_LIST "${STAGER_SQL_LIST}")
add_custom_target(stager_oracle_create.sql ALL
  COMMAND ${CMAKE_SOURCE_DIR}/tools/makeSqlScripts.sh stager ${CASTOR_DB_VERSION} ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}
  DEPENDS oracleSchema.sql oracleHeader.sql ${STAGER_SQL_LIST}
  COMMENT Creating stager_oracle_create.sql
)
CastorInstallSQL(stager_oracle_create.sql)
CastorInstallSQLFromSource(drop_oracle_schema.sql)
CastorInstallConfigNoRename(SQLSchemaTests.conf)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/castor-SQLSchemaTests.in.pl
  ${CMAKE_CURRENT_BINARY_DIR}/castor-SQLSchemaTests
  @ONLY)
CastorInstallScript(${CMAKE_CURRENT_BINARY_DIR}/castor-SQLSchemaTests)
