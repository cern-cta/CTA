/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/* 
 * File:   SQLiteSchemaComparer.cpp
 * Author: cedric
 * 
 * Created on December 10, 2019, 11:34 AM
 */

#include "SQLiteSchemaComparer.hpp"
#include "SQLiteSchemaInserter.hpp"
namespace cta {
namespace catalogue {
  
SQLiteSchemaComparer::SQLiteSchemaComparer(cta::rdbms::Conn &connection, cta::rdbms::Login &login, cta::catalogue::Catalogue& catalogue): SchemaComparer(connection,login,catalogue) {
}

SQLiteSchemaComparer::~SQLiteSchemaComparer() {
}

void SQLiteSchemaComparer::compare(){
  insertSchemaInSQLite();
  //compare
  //return the results
}

void SQLiteSchemaComparer::insertSchemaInSQLite() {
  std::map<std::string, uint64_t> schemaVersion = m_catalogue.getSchemaVersion();
  uint64_t schemaVersionMajor = schemaVersion.at("SCHEMA_VERSION_MAJOR");
  uint64_t schemaVersionMinor = schemaVersion.at("SCHEMA_VERSION_MINOR");
  std::string schemaVersionStr = std::to_string(schemaVersionMajor)+"."+std::to_string(schemaVersionMinor);
  cta::catalogue::SQLiteSchemaInserter schemaInserter(schemaVersionStr,m_login.dbType,"/home/cedric/CTA/catalogue/","/home/cedric/CTA-execute/sqliteDbFile");
  schemaInserter.insert();
}

}}