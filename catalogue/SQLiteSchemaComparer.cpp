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
#include <functional>

#include "SQLiteSchemaComparer.hpp"
#include "SQLiteSchemaInserter.hpp"
#include "common/log/DummyLogger.hpp"

namespace cta::catalogue {

SQLiteSchemaComparer::SQLiteSchemaComparer(const std::string& databaseToCheckName, DatabaseMetadataGetter &catalogueMetadataGetter): SchemaComparer(databaseToCheckName,catalogueMetadataGetter) {
  log::DummyLogger dl("dummy","dummy");
  auto login = rdbms::Login::parseString("in_memory");
  //Create SQLite connection
  m_sqliteConnPool.reset(new rdbms::ConnPool(login,1));
  m_sqliteConn = m_sqliteConnPool->getConn();
  //Create the Metadata getter
  auto sqliteCatalogueMetadataGetter = std::make_unique<SQLiteDatabaseMetadataGetter>(m_sqliteConn);
  //Create the Schema Metadata Getter that will filter the SQLite schema metadata according to the catalogue database type we would like to compare
  m_schemaMetadataGetter.reset(new SchemaMetadataGetter(std::move(sqliteCatalogueMetadataGetter),catalogueMetadataGetter.getDbType()));
}

SQLiteSchemaComparer::~SQLiteSchemaComparer() {
  //Release the connection before the connPool is deleted
  m_sqliteConn.reset();
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
  databaseTables.remove_if([&schemaTables](const std::string& catalogueTable){
    return std::find_if(schemaTables.begin(),schemaTables.end(),[&catalogueTable](std::string_view schemaTable){
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

SchemaCheckerResult SQLiteSchemaComparer::compareIndexes() {
  insertSchemaInSQLite();
  const Items catalogueIndexes = m_databaseMetadataGetter.getIndexNames();
  const Items schemaIndexes = m_schemaMetadataGetter->getIndexNames();
  return compareItems("INDEX", std::make_tuple(catalogueIndexes, Level::Warn), std::make_tuple(schemaIndexes, Level::Error));
}

SchemaCheckerResult SQLiteSchemaComparer::compareItems(const std::string &itemType, const LoggedItems& fromDatabase,
  const LoggedItems& fromSQLite) const {
  SchemaCheckerResult result;
  const auto& [itemsFromDatabase, logFromDataBase] = fromDatabase;
  const auto& [itemsFromSQLite, logFromSQLite] = fromSQLite;

  auto findMismatchs = [&result, &itemType](const Items& items, const Items& itemsToCompare, const std::string& message,
    const Level& logLevel) {
    std::function<void(const std::string&)> addResult;
    if (logLevel == Level::Error) addResult = [&result](const std::string& msg) {result.addError(msg);};
    if (logLevel == Level::Warn) addResult = [&result](const std::string& msg) {result.addWarning(msg);};
    for (auto &item : items) {
      if (std::find(itemsToCompare.begin(), itemsToCompare.end(), item) == itemsToCompare.end()) {
        addResult(itemType + " " + item + message);
      }
    }
  };

  std::string logMsg = " is missing in the schema but defined in the " + m_databaseToCheckName + " database.";
  findMismatchs(itemsFromDatabase, itemsFromSQLite, logMsg, logFromDataBase);
  logMsg = " is missing in the " + m_databaseToCheckName + " database but is defined in the schema.";
  findMismatchs(itemsFromSQLite, itemsFromDatabase, logMsg, logFromSQLite);

  return result;
}

SchemaCheckerResult SQLiteSchemaComparer::compareTables(const std::list<std::string>& databaseTables, const std::list<std::string>& schemaTables){
  SchemaCheckerResult result;
  std::map<std::string, std::map<std::string, std::string, std::less<>>, std::less<>> databaseTableColumns;
  std::map<std::string, std::map<std::string, std::string, std::less<>>, std::less<>> schemaTableColumns;
  std::map<std::string,std::list<std::string>,std::less<>> databaseTableConstraints;
  std::map<std::string, std::list<std::string>,std::less<>> schemaTableConstraints;

  for(auto &databaseTable: databaseTables){
    databaseTableColumns[databaseTable] = m_databaseMetadataGetter.getColumns(databaseTable);
    databaseTableConstraints[databaseTable] = m_databaseMetadataGetter.getConstraintNames(databaseTable);
  }

  for(auto &schemaTable: schemaTables){
    schemaTableColumns[schemaTable] = m_schemaMetadataGetter->getColumns(schemaTable);
    if(m_compareTableConstraints) {
      schemaTableConstraints[schemaTable] = m_schemaMetadataGetter->getConstraintNames(schemaTable);
      const LoggedItems databaseConstrains = std::make_tuple(databaseTableConstraints[schemaTable], Level::Error);
      const LoggedItems schemaConstrains = std::make_tuple(schemaTableConstraints[schemaTable], Level::Error);
      result += compareItems("IN TABLE " + schemaTable + ", CONSTRAINT", databaseConstrains, schemaConstrains);
    }
  }

  result += compareTableColumns(databaseTableColumns,m_databaseToCheckName+" database",schemaTableColumns,"schema");
  result += compareTableColumns(schemaTableColumns,"schema",databaseTableColumns,m_databaseToCheckName+" database");

  return result;
}

SchemaCheckerResult SQLiteSchemaComparer::compareTableColumns(const TableColumns & schema1TableColumns,
  const std::string &schema1Type,const TableColumns & schema2TableColumns, const std::string &schema2Type) const {
  SchemaCheckerResult result;
  for(auto &[schema1TableName, mapSchema1ColumnType]: schema1TableColumns){
    //For each firstSchema table, get the corresponding secondSchema table
    //If it does not exist, add a difference and go ahead
    //otherwise, get the columns/types of the table from the firstSchema and do the comparison
    //against the columns/types of the same table from the secondSchema
    try {
      std::map<std::string, std::string, std::less<>> mapSchema2ColumnType = schema2TableColumns.at(schema1TableName);
      for(const auto &[schema1ColumnName, schema1ColumnType]: mapSchema1ColumnType){
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

} // namespace cta::catalogue
