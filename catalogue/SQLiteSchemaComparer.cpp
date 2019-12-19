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
  
SQLiteSchemaComparer::SQLiteSchemaComparer(const cta::rdbms::Login::DbType &catalogueDbType, rdbms::Conn &catalogueConn, const std::string & allSchemasVersionPath): SchemaComparer(catalogueDbType,catalogueConn),m_allSchemasVersionPath(allSchemasVersionPath) {
  log::DummyLogger dl("dummy","dummy");
  auto login = rdbms::Login::parseString("in_memory");
  m_sqliteConnPool.reset(new rdbms::ConnPool(login,1));
  m_sqliteConn = std::move(m_sqliteConnPool->getConn());
  m_sqliteSchemaMetadataGetter.reset(new SQLiteCatalogueMetadataGetter(m_sqliteConn));
}

SQLiteSchemaComparer::~SQLiteSchemaComparer() {
  //Release the connection before the connPool is deleted
  m_sqliteConn.~Conn();
  m_sqliteConnPool.reset();
}

SchemaComparerResult SQLiteSchemaComparer::compare(){
  SchemaComparerResult res;
  insertSchemaInSQLite();
  res += compareIndexes();
  res += compareTables();
  return res;
}

SchemaComparerResult SQLiteSchemaComparer::compareTables(){
  std::list<std::string> catalogueTables = m_catalogueMetadataGetter->getTableNames();
  std::list<std::string> schemaTables = m_sqliteSchemaMetadataGetter->getTableNames();
  SchemaComparerResult res = compareTables(catalogueTables,schemaTables);
  return res;
}

void SQLiteSchemaComparer::insertSchemaInSQLite() {
  cta::catalogue::SQLiteSchemaInserter schemaInserter(m_catalogueSchemaVersion,m_dbType,m_allSchemasVersionPath,m_sqliteConn);
  schemaInserter.insert();
}

SchemaComparerResult SQLiteSchemaComparer::compareIndexes(){
  std::list<std::string> catalogueIndexes = m_catalogueMetadataGetter->getIndexNames();
  std::list<std::string> schemaIndexes = m_sqliteSchemaMetadataGetter->getIndexNames();
  return compareItems("INDEX", catalogueIndexes, schemaIndexes);
}

SchemaComparerResult SQLiteSchemaComparer::compareItems(const std::string &itemType, const std::list<std::string>& itemsFromCatalogue, const std::list<std::string>& itemsFromSQLite){
  SchemaComparerResult result;
  for(auto &catalogueItem: itemsFromCatalogue){
    if(std::find(itemsFromSQLite.begin(),itemsFromSQLite.end(),catalogueItem) == itemsFromSQLite.end()){
      result.addDiff(itemType+" "+catalogueItem+" is missing in the schema but defined in the catalogue");
    }
  }
  for(auto &sqliteItem: itemsFromSQLite){
    if(std::find(itemsFromCatalogue.begin(),itemsFromCatalogue.end(),sqliteItem) == itemsFromCatalogue.end()){
      result.addDiff(itemType+" "+sqliteItem+" is missing in the catalogue but is defined in the schema");
    }
  }
  return result;
}

SchemaComparerResult SQLiteSchemaComparer::compareTables(const std::list<std::string>& catalogueTables, const std::list<std::string>& schemaTables){
  SchemaComparerResult result;
  std::map<std::string, std::map<std::string, std::string>> catalogueTableColumns;
  std::map<std::string, std::map<std::string, std::string>> schemaTableColumns;
  for(auto &catalogueTable: catalogueTables){
    catalogueTableColumns[catalogueTable] = m_catalogueMetadataGetter->getColumns(catalogueTable);
  }
  
  for(auto &schemaTable: schemaTables){
    schemaTableColumns[schemaTable] = m_sqliteSchemaMetadataGetter->getColumns(schemaTable);
  }
  
  result +=  compareTableColumns(catalogueTableColumns,"catalogue",schemaTableColumns,"schema");
  result +=  compareTableColumns(schemaTableColumns,"schema",catalogueTableColumns,"catalogue");
  
  return result;
}

SchemaComparerResult SQLiteSchemaComparer::compareTableColumns(const TableColumns & firstSchemaTableColumns, const std::string &schema1Type,const TableColumns & secondSchemaTableColumns, const std::string &schema2Type){
  SchemaComparerResult result;
  for(auto &kvFirstSchemaTableColumns: firstSchemaTableColumns){
    //For each firstSchema table, get the corresponding secondSchema table
    //If it does not exist, add a difference and go ahead
    //otherwise, get the columns/types of the table from the firstSchema and do the comparison
    //against the columns/types of the same table from the secondSchema
    std::string firstSchemaTableName = kvFirstSchemaTableColumns.first;
    try {
      std::map<std::string, std::string> mapSecondSchemaColumnType = secondSchemaTableColumns.at(firstSchemaTableName);
      std::map<std::string, std::string> mapFirstSchemaColumnType = kvFirstSchemaTableColumns.second;
      for(auto &kvFirstSchemaColumn: mapFirstSchemaColumnType){
        std::string firstSchemaColumnName = kvFirstSchemaColumn.first;
        std::string firstSchemaColumnType = kvFirstSchemaColumn.second;
        try {
          std::string schemaColumnType = mapSecondSchemaColumnType.at(firstSchemaColumnName);
          if( firstSchemaColumnType != schemaColumnType){
            result.addDiff("TABLE "+firstSchemaTableName+" from "+schema1Type+" has a column named "+firstSchemaColumnName+" that has a type "+firstSchemaColumnType+" that does not match the column type from the "+schema2Type+" ("+schemaColumnType+")");
          }
        } catch (const std::out_of_range &) {
          result.addDiff("TABLE "+firstSchemaTableName+" from "+schema1Type+" has a column named " + firstSchemaColumnName + " that is missing in the "+schema2Type+".");
        }
      }
    } catch (const std::out_of_range &) {
      result.addDiff("TABLE "+firstSchemaTableName+" is missing in the "+schema2Type+" but is defined in the "+schema1Type+".");
    }
  }
  return result;
}

}}