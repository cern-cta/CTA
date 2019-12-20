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

#include "CatalogueMetadataGetter.hpp"
#include <algorithm>

namespace cta {
namespace catalogue {

void CatalogueMetadataGetter::removeObjectNameContaining(std::list<std::string>& objects, const std::list<std::string> &wordsToTriggerRemoval){
  objects.remove_if([&wordsToTriggerRemoval](const std::string &object){
    return std::find_if(wordsToTriggerRemoval.begin(), wordsToTriggerRemoval.end(),[&object](const std::string &wordTriggeringRemoval){
      return object.find(wordTriggeringRemoval) != std::string::npos;
    }) != wordsToTriggerRemoval.end();
  });
}

void CatalogueMetadataGetter::removeObjectNameNotContaining(std::list<std::string>& objects, const std::list<std::string> &wordsNotToTriggerRemoval){
  objects.remove_if([&wordsNotToTriggerRemoval](const std::string &object){
    return std::find_if(wordsNotToTriggerRemoval.begin(), wordsNotToTriggerRemoval.end(),[&object](const std::string &wordsNotToTriggeringRemoval){
      return object.find(wordsNotToTriggeringRemoval) == std::string::npos;
    }) != wordsNotToTriggerRemoval.end();
  });
}
  
CatalogueMetadataGetter::CatalogueMetadataGetter(cta::rdbms::Conn& conn):m_conn(conn){}

std::string CatalogueMetadataGetter::getCatalogueVersion(){
  std::string schemaVersion;
  const char *const sql =
    "SELECT "
      "CTA_CATALOGUE.SCHEMA_VERSION_MAJOR AS SCHEMA_VERSION_MAJOR,"
      "CTA_CATALOGUE.SCHEMA_VERSION_MINOR AS SCHEMA_VERSION_MINOR "
    "FROM "
      "CTA_CATALOGUE";

  auto stmt = m_conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  if(rset.next()) {
    schemaVersion += std::to_string(rset.columnUint64("SCHEMA_VERSION_MAJOR"));
    schemaVersion += ".";
    schemaVersion += std::to_string(rset.columnUint64("SCHEMA_VERSION_MINOR"));
    return schemaVersion;
  } else {
    throw exception::Exception("SCHEMA_VERSION_MAJOR,SCHEMA_VERSION_MINOR not found in the CTA_CATALOGUE");
  }
}

std::list<std::string> CatalogueMetadataGetter::getTableNames(){
  return m_conn.getTableNames();
}

std::list<std::string> CatalogueMetadataGetter::getIndexNames(){
  std::list<std::string> indexNames = m_conn.getIndexNames();
  removeObjectNameNotContaining(indexNames,{"_IDX"});
  return indexNames;
}

std::map<std::string,std::string> CatalogueMetadataGetter::getColumns(const std::string& tableName){
  return m_conn.getColumns(tableName);
}

CatalogueMetadataGetter::~CatalogueMetadataGetter() {}

SQLiteCatalogueMetadataGetter::SQLiteCatalogueMetadataGetter(cta::rdbms::Conn & conn):CatalogueMetadataGetter(conn){}
SQLiteCatalogueMetadataGetter::~SQLiteCatalogueMetadataGetter(){}

std::list<std::string> SQLiteCatalogueMetadataGetter::getIndexNames() {
  std::list<std::string> indexNames = CatalogueMetadataGetter::getIndexNames();
  removeObjectNameContaining(indexNames,{"sqlite_autoindex"});
  return indexNames;
}

std::list<std::string> SQLiteCatalogueMetadataGetter::getTableNames(){
  std::list<std::string> tableNames = CatalogueMetadataGetter::getTableNames();
  removeObjectNameContaining(tableNames,{"sqlite_sequence"});
  return tableNames;
}

std::map<std::string, std::string> SQLiteCatalogueMetadataGetter::getColumns(const std::string& tableName){
  return CatalogueMetadataGetter::getColumns(tableName);
}

OracleCatalogueMetadataGetter::OracleCatalogueMetadataGetter(cta::rdbms::Conn & conn):CatalogueMetadataGetter(conn){}
OracleCatalogueMetadataGetter::~OracleCatalogueMetadataGetter(){}

std::list<std::string> OracleCatalogueMetadataGetter::getIndexNames() {
  return CatalogueMetadataGetter::getIndexNames();
}

std::list<std::string> OracleCatalogueMetadataGetter::getTableNames() {
  std::list<std::string> tableNames = CatalogueMetadataGetter::getTableNames();
  return tableNames;
}

std::map<std::string, std::string> OracleCatalogueMetadataGetter::getColumns(const std::string& tableName){
  return CatalogueMetadataGetter::getColumns(tableName);
}

MySQLCatalogueMetadataGetter::MySQLCatalogueMetadataGetter(cta::rdbms::Conn& conn):CatalogueMetadataGetter(conn) {}
MySQLCatalogueMetadataGetter::~MySQLCatalogueMetadataGetter(){}

std::list<std::string> MySQLCatalogueMetadataGetter::getIndexNames() {
  return CatalogueMetadataGetter::getIndexNames();
}

std::list<std::string> MySQLCatalogueMetadataGetter::getTableNames() {
  return CatalogueMetadataGetter::getTableNames();
}

std::map<std::string, std::string> MySQLCatalogueMetadataGetter::getColumns(const std::string& tableName){
  return CatalogueMetadataGetter::getColumns(tableName);
}

PostgresCatalogueMetadataGetter::PostgresCatalogueMetadataGetter(cta::rdbms::Conn& conn):CatalogueMetadataGetter(conn) {}
PostgresCatalogueMetadataGetter::~PostgresCatalogueMetadataGetter(){}

std::list<std::string> PostgresCatalogueMetadataGetter::getIndexNames() {
  return CatalogueMetadataGetter::getIndexNames();
}

std::list<std::string> PostgresCatalogueMetadataGetter::getTableNames() {
  return CatalogueMetadataGetter::getTableNames();
}

std::map<std::string, std::string> PostgresCatalogueMetadataGetter::getColumns(const std::string& tableName){
  return CatalogueMetadataGetter::getColumns(tableName);
}

CatalogueMetadataGetter * CatalogueMetadataGetterFactory::create(const rdbms::Login::DbType dbType, cta::rdbms::Conn & conn) {
  typedef rdbms::Login::DbType DbType;
  switch(dbType){
    case DbType::DBTYPE_IN_MEMORY:
    case DbType::DBTYPE_SQLITE:
      return new SQLiteCatalogueMetadataGetter(conn);
    case DbType::DBTYPE_ORACLE:
      return new OracleCatalogueMetadataGetter(conn);
    case DbType::DBTYPE_MYSQL:
      return new MySQLCatalogueMetadataGetter(conn);
    case DbType::DBTYPE_POSTGRESQL:
      return new PostgresCatalogueMetadataGetter(conn);
    default:
      throw cta::exception::Exception("In CatalogueMetadataGetterFactory::create(), can't get CatalogueMetadataGetter for dbType "+rdbms::Login::dbTypeToString(dbType));
  }
}


}}