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

namespace cta {
namespace catalogue {

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

CatalogueMetadataGetter::~CatalogueMetadataGetter() {}

SQLiteCatalogueMetadataGetter::SQLiteCatalogueMetadataGetter(cta::rdbms::Conn & conn):CatalogueMetadataGetter(conn){}
SQLiteCatalogueMetadataGetter::~SQLiteCatalogueMetadataGetter(){}

std::list<std::string> SQLiteCatalogueMetadataGetter::getIndexNames() {
  std::list<std::string> indexNames = m_conn.getIndexNames();
  indexNames.remove_if([](std::string& indexName){
    return ((indexName.find("sqlite_autoindex") != std::string::npos)); 
  });
  return indexNames;
}

std::list<std::string> SQLiteCatalogueMetadataGetter::getTableNames(){
  return std::list<std::string>();
}

OracleCatalogueMetadataGetter::OracleCatalogueMetadataGetter(cta::rdbms::Conn & conn):CatalogueMetadataGetter(conn){}
OracleCatalogueMetadataGetter::~OracleCatalogueMetadataGetter(){}

std::list<std::string> OracleCatalogueMetadataGetter::getIndexNames() {
  std::list<std::string> indexNames = m_conn.getIndexNames();
  indexNames.remove_if([](std::string& indexName){
    return ((indexName.find("_UN") != std::string::npos) || (indexName.find("PK") != std::string::npos) || (indexName.find("_LLN") != std::string::npos)); 
  });
  return indexNames;
}

std::list<std::string> OracleCatalogueMetadataGetter::getTableNames() {
  return std::list<std::string>();
}



CatalogueMetadataGetter * CatalogueMetadataGetterFactory::create(const rdbms::Login::DbType dbType, cta::rdbms::Conn & conn) {
  typedef rdbms::Login::DbType DbType;
  switch(dbType){
    case DbType::DBTYPE_IN_MEMORY:
    case DbType::DBTYPE_SQLITE:
      return new SQLiteCatalogueMetadataGetter(conn);
    case DbType::DBTYPE_ORACLE:
      return new OracleCatalogueMetadataGetter(conn);
    default:
      return nullptr;
  }
}


}}