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

CatalogueMetadataGetter::CatalogueMetadataGetter(cta::rdbms::ConnPool & connPool):m_connPool(connPool){}

CatalogueMetadataGetter::~CatalogueMetadataGetter() {}

SQLiteCatalogueMetadataGetter::SQLiteCatalogueMetadataGetter(cta::rdbms::ConnPool & connPool):CatalogueMetadataGetter(connPool){}
SQLiteCatalogueMetadataGetter::~SQLiteCatalogueMetadataGetter(){}

std::list<std::string> SQLiteCatalogueMetadataGetter::getIndexNames() {
  rdbms::Conn connection = m_connPool.getConn();
  std::list<std::string> indexNames = connection.getIndexNames();
  connection.closeUnderlyingStmtsAndConn();
  indexNames.remove_if([](std::string& indexName){
    return ((indexName.find("sqlite_autoindex") != std::string::npos)); 
  });
  return indexNames;
}

OracleCatalogueMetadataGetter::OracleCatalogueMetadataGetter(cta::rdbms::ConnPool & connPool):CatalogueMetadataGetter(connPool){}
OracleCatalogueMetadataGetter::~OracleCatalogueMetadataGetter(){}

std::list<std::string> OracleCatalogueMetadataGetter::getIndexNames() {
  rdbms::Conn connection = m_connPool.getConn();
  std::list<std::string> indexNames = connection.getIndexNames();
  connection.closeUnderlyingStmtsAndConn();
  indexNames.remove_if([](std::string& indexName){
    return ((indexName.find("_UN") != std::string::npos) || (indexName.find("PK") != std::string::npos) || (indexName.find("_LLN") != std::string::npos)); 
  });
  return indexNames;
}


CatalogueMetadataGetter * CatalogueMetadataGetterFactory::create(const rdbms::Login::DbType dbType, cta::rdbms::ConnPool & connPool) {
  typedef rdbms::Login::DbType DbType;
  switch(dbType){
    case DbType::DBTYPE_IN_MEMORY:
    case DbType::DBTYPE_SQLITE:
      return new SQLiteCatalogueMetadataGetter(connPool);
    case DbType::DBTYPE_ORACLE:
      return new OracleCatalogueMetadataGetter(connPool);
    default:
      return nullptr;
  }
}


}}