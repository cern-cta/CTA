/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <algorithm>

#include "SQLiteSchemaComparer.hpp"
#include "SQLiteSchemaInserter.hpp"
#include "common/log/DummyLogger.hpp"

namespace cta {
namespace catalogue {

SQLiteSchemaComparer::SQLiteSchemaComparer(const std::string& databaseToCheckName, DatabaseMetadataGetter &catalogueMetadataGetter): SchemaComparer(databaseToCheckName,catalogueMetadataGetter) {
  log::DummyLogger dl("dummy","dummy");
  auto login = rdbms::Login::parseString("in_memory");
  //Create SQLite connection
  m_sqliteConnPool.reset(new rdbms::ConnPool(login,1));
  m_sqliteConn = std::move(m_sqliteConnPool->getConn());
  //Create the Metadata getter
  std::unique_ptr<SQLiteDatabaseMetadataGetter> sqliteCatalogueMetadataGetter(new SQLiteDatabaseMetadataGetter(m_sqliteConn));
  //Create the Schema Metadata Getter that will filter the SQLite schema metadata according to the catalogue database type we would like to compare
  m_schemaMetadataGetter.reset(new SchemaMetadataGetter(std::move(sqliteCatalogueMetadataGetter),catalogueMetadataGetter.getDbType()));
}

SQLiteSchemaComparer::~SQLiteSchemaComparer() {
  //Release the connection before the connPool is deleted
  m_sqliteConn.~Conn();
  m_sqliteConnPool.reset();
}

SchemaCheckerResult SQLiteSchemaComparer::compareAll(){
  insertSchemaInSQLite();
  SchemaCheckerResult res;
  res += compareTables();
  res += compareIndexes();
  return res;
}

SchemaCheckerResult SQLiteSchemaComparer::compareTables(){
  insertSchemaInSQLite();
  std::list<std::string> catalogueTables = m_databaseMetadataGetter.getTableNames();
  std::list<std::string> schemaTables = m_schemaMetadataGetter->getTableNames();
  SchemaCheckerResult res = compareTables(catalogueTables,schemaTables);
  return res;
}

SchemaCheckerResult SQLiteSchemaComparer::compareTablesLocatedInSchema(){
  insertSchemaInSQLite();
  std::list<std::string> databaseTables = m_databaseMetadataGetter.getTableNames();
  std::list<std::string> schemaTables = m_schemaMetadataGetter->getTableNames();
  databaseTables.remove_if([schemaTables](const std::string& catalogueTable){
    return std::find_if(schemaTables.begin(),schemaTables.end(),[catalogueTable](const std::string& schemaTable){
      return schemaTable == catalogueTable;
    }) == schemaTables.end();
  });
  SchemaCheckerResult res = compareTables(databaseTables,schemaTables);
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

SchemaCheckerResult SQLiteSchemaComparer::compareIndexes(){
  insertSchemaInSQLite();
  std::list<std::string> catalogueIndexes = m_databaseMetadataGetter.getIndexNames();
  std::list<std::string> schemaIndexes = m_schemaMetadataGetter->getIndexNames();
  return compareItems("INDEX", catalogueIndexes, schemaIndexes);
}

SchemaCheckerResult SQLiteSchemaComparer::compareItems(const std::string &itemType, const std::list<std::string>& itemsFromDatabase, const std::list<std::string>& itemsFromSQLite){
  SchemaCheckerResult result;
  for(auto &databaseItem: itemsFromDatabase){
    if(std::find(itemsFromSQLite.begin(),itemsFromSQLite.end(),databaseItem) == itemsFromSQLite.end()){
      result.addError(itemType+" "+databaseItem+" is missing in the schema but defined in the "+m_databaseToCheckName+" database.");
    }
  }
  for(auto &sqliteItem: itemsFromSQLite){
    if(std::find(itemsFromDatabase.begin(),itemsFromDatabase.end(),sqliteItem) == itemsFromDatabase.end()){
      result.addError(itemType+" "+sqliteItem+" is missing in the "+m_databaseToCheckName+" database but is defined in the schema.");
    }
  }
  return result;
}

SchemaCheckerResult SQLiteSchemaComparer::compareTables(const std::list<std::string>& databaseTables, const std::list<std::string>& schemaTables){
  SchemaCheckerResult result;
  std::map<std::string, std::map<std::string, std::string>> databaseTableColumns;
  std::map<std::string, std::map<std::string, std::string>> schemaTableColumns;
  std::map<std::string,std::list<std::string>> databaseTableConstraints;
  std::map<std::string, std::list<std::string>> schemaTableConstraints;

  for(auto &databaseTable: databaseTables){
    databaseTableColumns[databaseTable] = m_databaseMetadataGetter.getColumns(databaseTable);
    databaseTableConstraints[databaseTable] = m_databaseMetadataGetter.getConstraintNames(databaseTable);
  }

  for(auto &schemaTable: schemaTables){
    schemaTableColumns[schemaTable] = m_schemaMetadataGetter->getColumns(schemaTable);
    if(m_compareTableConstraints) {
      schemaTableConstraints[schemaTable] = m_schemaMetadataGetter->getConstraintNames(schemaTable);
      result += compareItems("IN TABLE "+schemaTable+", CONSTRAINT",databaseTableConstraints[schemaTable],schemaTableConstraints[schemaTable]);
    }
  }

  result += compareTableColumns(databaseTableColumns,m_databaseToCheckName+" database",schemaTableColumns,"schema");
  result += compareTableColumns(schemaTableColumns,"schema",databaseTableColumns,m_databaseToCheckName+" database");

  return result;
}

SchemaCheckerResult SQLiteSchemaComparer::compareTableColumns(const TableColumns & schema1TableColumns, const std::string &schema1Type,const TableColumns & schema2TableColumns, const std::string &schema2Type){
  SchemaCheckerResult result;
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
            result.addError("TABLE "+schema1TableName+" from "+schema1Type+" has a column named "+schema1ColumnName+" that has a type "+schema1ColumnType+" that does not match the column type from the "+schema2Type+" ("+schemaColumnType+")");
          }
        } catch (const std::out_of_range &) {
          result.addError("TABLE "+schema1TableName+" from "+schema1Type+" has a column named " + schema1ColumnName + " that is missing in the "+schema2Type+".");
        }
      }
    } catch (const std::out_of_range &) {
      result.addError("TABLE "+schema1TableName+" is missing in the "+schema2Type+" but is defined in the "+schema1Type+".");
    }
  }
  return result;
}

}}
