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
  
SQLiteSchemaComparer::SQLiteSchemaComparer(CatalogueMetadataGetter &catalogueMetadataGetter): SchemaComparer(catalogueMetadataGetter) {
  log::DummyLogger dl("dummy","dummy");
  auto login = rdbms::Login::parseString("in_memory");
  //Create SQLite connection
  m_sqliteConnPool.reset(new rdbms::ConnPool(login,1));
  m_sqliteConn = std::move(m_sqliteConnPool->getConn());
  //Create the Metadata getter
  std::unique_ptr<SQLiteCatalogueMetadataGetter> sqliteCatalogueMetadataGetter(new SQLiteCatalogueMetadataGetter(m_sqliteConn));
  //Create the Schema Metadata Getter that will filter the SQLite schema metadata according to the catalogue database type we would like to compare
  m_schemaMetadataGetter.reset(new SchemaMetadataGetter(std::move(sqliteCatalogueMetadataGetter),catalogueMetadataGetter.getDbType()));
}

SQLiteSchemaComparer::~SQLiteSchemaComparer() {
  //Release the connection before the connPool is deleted
  m_sqliteConn.~Conn();
  m_sqliteConnPool.reset();
}

SchemaComparerResult SQLiteSchemaComparer::compareAll(){
  insertSchemaInSQLite();
  SchemaComparerResult res;
  res += compareTables();
  res += compareIndexes();
  return res;
}

SchemaComparerResult SQLiteSchemaComparer::compareTables(){
  insertSchemaInSQLite();
  std::list<std::string> catalogueTables = m_catalogueMetadataGetter.getTableNames();
  std::list<std::string> schemaTables = m_schemaMetadataGetter->getTableNames();
  SchemaComparerResult res = compareTables(catalogueTables,schemaTables);
  return res;
}

void SQLiteSchemaComparer::insertSchemaInSQLite() {
  if(!m_isSchemaInserted){
    if(m_schemaSqlStatementsReader != nullptr){
      cta::catalogue::SQLiteSchemaInserter schemaInserter(m_sqliteConn);
      schemaInserter.insert(m_schemaSqlStatementsReader->getStatements());
    } else {
      throw cta::exception::Exception("In SQLiteSchemaComparer::insertSchemaInSQLite(): unable to insert schema in sqlite because no SchemaSqlStatementReader has been set.");
    }
  }
  m_isSchemaInserted = true;
}

SchemaComparerResult SQLiteSchemaComparer::compareIndexes(){
  insertSchemaInSQLite();
  std::list<std::string> catalogueIndexes = m_catalogueMetadataGetter.getIndexNames();
  std::list<std::string> schemaIndexes = m_schemaMetadataGetter->getIndexNames();
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
  std::map<std::string,std::list<std::string>> catalogueTableConstraints;
  std::map<std::string, std::list<std::string>> schemaTableConstraints;
  
  for(auto &catalogueTable: catalogueTables){
    catalogueTableColumns[catalogueTable] = m_catalogueMetadataGetter.getColumns(catalogueTable);
    catalogueTableConstraints[catalogueTable] = m_catalogueMetadataGetter.getConstraintNames(catalogueTable);
  }
  
  for(auto &schemaTable: schemaTables){
    schemaTableColumns[schemaTable] = m_schemaMetadataGetter->getColumns(schemaTable);
    if(m_compareTableConstraints) {
      schemaTableConstraints[schemaTable] = m_schemaMetadataGetter->getConstraintNames(schemaTable);
      result += compareItems("IN TABLE "+schemaTable+", CONSTRAINT",catalogueTableConstraints[schemaTable],schemaTableConstraints[schemaTable]);
    }
  }
  
  result += compareTableColumns(catalogueTableColumns,"catalogue",schemaTableColumns,"schema");
  result += compareTableColumns(schemaTableColumns,"schema",catalogueTableColumns,"catalogue");
  
  return result;
}

SchemaComparerResult SQLiteSchemaComparer::compareTableColumns(const TableColumns & schema1TableColumns, const std::string &schema1Type,const TableColumns & schema2TableColumns, const std::string &schema2Type){
  SchemaComparerResult result;
  for(auto &kvFirstSchemaTableColumns: schema1TableColumns){
    //For each firstSchema table, get the corresponding secondSchema table
    //If it does not exist, add a difference and go ahead
    //otherwise, get the columns/types of the table from the firstSchema and do the comparison
    //against the columns/types of the same table from the secondSchema
    std::string schema1TableName = kvFirstSchemaTableColumns.first;
    try {
      std::map<std::string, std::string> mapSchema2ColumnType = schema2TableColumns.at(schema1TableName);
      std::map<std::string, std::string> mapSchema1ColumnType = kvFirstSchemaTableColumns.second;
      for(auto &kvFirstSchemaColumn: mapSchema1ColumnType){
        std::string schema1ColumnName = kvFirstSchemaColumn.first;
        std::string schema1ColumnType = kvFirstSchemaColumn.second;
        try {
          std::string schemaColumnType = mapSchema2ColumnType.at(schema1ColumnName);
          if( schema1ColumnType != schemaColumnType){
            result.addDiff("TABLE "+schema1TableName+" from "+schema1Type+" has a column named "+schema1ColumnName+" that has a type "+schema1ColumnType+" that does not match the column type from the "+schema2Type+" ("+schemaColumnType+")");
          }
        } catch (const std::out_of_range &) {
          result.addDiff("TABLE "+schema1TableName+" from "+schema1Type+" has a column named " + schema1ColumnName + " that is missing in the "+schema2Type+".");
        }
      }
    } catch (const std::out_of_range &) {
      result.addDiff("TABLE "+schema1TableName+" is missing in the "+schema2Type+" but is defined in the "+schema1Type+".");
    }
  }
  return result;
}

}}