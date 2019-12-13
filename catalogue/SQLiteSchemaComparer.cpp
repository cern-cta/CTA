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

#include <algorithm>

#include "SQLiteSchemaComparer.hpp"
#include "SQLiteSchemaInserter.hpp"
#include "common/log/DummyLogger.hpp"

namespace cta {
namespace catalogue {
  
SQLiteSchemaComparer::SQLiteSchemaComparer(const cta::rdbms::Login::DbType &catalogueDbType, const std::string &schemaVersion, rdbms::ConnPool &catalogueConnPool): SchemaComparer(catalogueDbType,schemaVersion,catalogueConnPool) {
  log::DummyLogger dl("dummy","dummy");
  
  auto login = rdbms::Login::parseString("in_memory");
  m_sqliteConnPool.reset(new rdbms::ConnPool(login,1));
  m_catalogueMetadataGetter.reset(CatalogueMetadataGetterFactory::create(catalogueDbType,catalogueConnPool));
  m_sqliteSchemaMetadataGetter.reset(new SQLiteCatalogueMetadataGetter(*m_sqliteConnPool));
}

SQLiteSchemaComparer::~SQLiteSchemaComparer() {
}

SchemaComparerResult SQLiteSchemaComparer::compare(){
  SchemaComparerResult res;
  insertSchemaInSQLite();
  res += compareIndexes();
  return res;
}

SchemaComparerResult SQLiteSchemaComparer::compareTables(){
  return SchemaComparerResult();
}

void SQLiteSchemaComparer::insertSchemaInSQLite() {
  cta::catalogue::SQLiteSchemaInserter schemaInserter(m_schemaVersion,m_dbType,"/home/cedric/CTA/catalogue/",*m_sqliteConnPool);
  schemaInserter.insert();
}

SchemaComparerResult SQLiteSchemaComparer::compareIndexes(){
  SchemaComparerResult result;
  std::list<std::string> catalogueIndexes = m_catalogueMetadataGetter->getIndexNames();
  std::list<std::string> schemaIndexes = m_sqliteSchemaMetadataGetter->getIndexNames();
  for(auto &ci: catalogueIndexes){
    if(std::find(schemaIndexes.begin(),schemaIndexes.end(),ci) == schemaIndexes.end()){
      result.addDiff("INDEX "+ci+" is missing in the schema but defined in the catalogue");
    }
  }
  for(auto &si: schemaIndexes){
    if(std::find(catalogueIndexes.begin(),catalogueIndexes.end(),si) == catalogueIndexes.end()){
      result.addDiff("INDEX "+si+" is missing in the catalogue but is defined in the schema");
    }
  }
  return result;
}

}}