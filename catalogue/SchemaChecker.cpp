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

#include "SchemaChecker.hpp"
#include "SQLiteSchemaComparer.hpp"

namespace cta::catalogue {

SchemaChecker::SchemaChecker(const std::string& databaseToCheckName, rdbms::Login::DbType dbType, cta::rdbms::Conn &conn)
  : m_databaseToCheckName(databaseToCheckName),
    m_dbType(dbType),
    m_catalogueConn(conn) {
  m_databaseMetadataGetter.reset(DatabaseMetadataGetterFactory::create(m_dbType, m_catalogueConn));
}

void SchemaChecker::checkSchemaComparerNotNull(const std::string& methodName){
  if(m_schemaComparer == nullptr){
    std::string exceptionMsg = "In "+methodName+", No schema comparer used. Please specify the schema comparer by using the methods useXXXXSchemaComparer()";
    throw cta::exception::Exception(exceptionMsg);
  }
}

SchemaCheckerResult SchemaChecker::displayingCompareSchema(std::ostream & stdOut, std::ostream & stdErr){
  checkSchemaComparerNotNull(__PRETTY_FUNCTION__);
  SchemaCheckerResult totalResult;
  stdOut << "Schema version : " << m_databaseMetadataGetter->getCatalogueVersion().getSchemaVersion<std::string>() << std::endl;
  stdOut << "Checking indexes..." << std::endl;
  cta::catalogue::SchemaCheckerResult resIndex = m_schemaComparer->compareIndexes();
  totalResult += resIndex;
  resIndex.displayErrors(stdErr);
  stdOut <<"  "<<resIndex.statusToString(resIndex.getStatus())<<std::endl;
    stdOut << "Checking tables, columns and constraints..." << std::endl;
  cta::catalogue::SchemaCheckerResult resTables = m_schemaComparer->compareTables();
  totalResult += resTables;
  resTables.displayErrors(stdErr);
  stdOut <<"  "<<resTables.statusToString(resTables.getStatus())<<std::endl;
  stdOut << "Status of the checking : " << cta::catalogue::SchemaCheckerResult::statusToString(totalResult.getStatus()) << std::endl;
  return totalResult;
}

SchemaCheckerResult SchemaChecker::warnParallelTables(){
  SchemaCheckerResult res;
  std::list<std::string> parallelTables = m_databaseMetadataGetter->getParallelTableNames();
  for(auto& table:parallelTables) {
    std::string warning = "TABLE " + table + " is set as PARALLEL";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnSchemaUpgrading(){
  SchemaCheckerResult res;
  SchemaVersion catalogueVersion = m_databaseMetadataGetter->getCatalogueVersion();
  if(catalogueVersion.getStatus<SchemaVersion::Status>() == SchemaVersion::Status::UPGRADING){
    std::string warning = "The status of the schema is " + catalogueVersion.getStatus<std::string>() + ", the future version is " + catalogueVersion.getSchemaVersionNext<std::string>();
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::compareTablesLocatedInSchema(){
  checkSchemaComparerNotNull(__PRETTY_FUNCTION__);
  return m_schemaComparer->compareTablesLocatedInSchema();
}

SchemaCheckerResult SchemaChecker::checkTableContainsColumns(const std::string& tableName, const std::list<std::string>& columnNames){
  std::map<std::string, std::string> mapColumnsTypes = m_databaseMetadataGetter->getColumns(tableName);
  SchemaCheckerResult res;
  if(mapColumnsTypes.empty()){
    std::string error = "TABLE " + tableName +" does not exist.";
    res.addError(error);
    return res;
  }
  for(auto &columnName: columnNames){
    if(mapColumnsTypes.find(columnName) == mapColumnsTypes.end()) {
      std::string error = "TABLE " + tableName + " does not contain the column " + columnName + ".";
      res.addError(error);
    }
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnProcedures() {
  SchemaCheckerResult res;
  std::list<std::string> procedureNames = m_databaseMetadataGetter->getStoredProcedures();
  for(auto & procedure: procedureNames){
    std::string warning = "PROCEDURE " + procedure + " exists in the " + m_databaseToCheckName + " database";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnSynonyms() {
  SchemaCheckerResult res;
  std::list<std::string> synonymsNames = m_databaseMetadataGetter->getSynonyms();
  for(auto & synonym: synonymsNames){
    std::string warning = "SYNONYM " + synonym + " exists in the " + m_databaseToCheckName + " database";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnTypes() {
  SchemaCheckerResult res;
  std::list<std::string> typeNames = m_databaseMetadataGetter->getTypes();
  for(auto & type: typeNames) {
    std::string warning = "TYPE " + type + " exists in the " + m_databaseToCheckName + " database";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnErrorLoggingTables() {
  SchemaCheckerResult res;
  std::list<std::string> errorTables = m_databaseMetadataGetter->getErrorLoggingTables();
  for(auto & errorTable: errorTables) {
    std::string warning = "Error logging table " + errorTable + " exists in the " + m_databaseToCheckName + " database";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnMissingIndexes() {
  SchemaCheckerResult res;
  auto missingIndexes = m_databaseMetadataGetter->getMissingIndexes();
  for(auto& missingIndex : missingIndexes) {
    std::string warning = "Column " + missingIndex + " is part of a foreign key constraint but has no index";
    res.addWarning(warning);
  }
  return res;
}

/////////////////////////////////////////
// SchemaChecker::Builder
/////////////////////////////////////////
SchemaChecker::Builder::Builder(const std::string& databaseToCheckName, const cta::rdbms::Login::DbType& dbType,
  cta::rdbms::Conn & conn)
  : m_databaseToCheckName(databaseToCheckName),
    m_dbType(dbType),
    m_catalogueConn(conn) {
  m_databaseMetadataGetter.reset(DatabaseMetadataGetterFactory::create(m_dbType, m_catalogueConn));
}

SchemaChecker::Builder & SchemaChecker::Builder::useSQLiteSchemaComparer(){
  m_schemaComparer.reset(new SQLiteSchemaComparer(m_databaseToCheckName,*m_databaseMetadataGetter));
  return *this;
}

SchemaChecker::Builder& SchemaChecker::Builder::useDirectorySchemaReader(const std::string& allSchemasVersionsDirectory) {
    m_schemaSqlStatementsReader.reset(new DirectoryVersionsSqlStatementsReader(m_dbType,m_databaseMetadataGetter->getCatalogueVersion().getSchemaVersion<std::string>(),allSchemasVersionsDirectory));
    return *this;
}

SchemaChecker::Builder& SchemaChecker::Builder::useMapStatementsReader() {
  m_schemaSqlStatementsReader.reset(new MapSqlStatementsReader(m_dbType,m_databaseMetadataGetter->getCatalogueVersion().getSchemaVersion<std::string>()));
  return *this;
}

SchemaChecker::Builder& SchemaChecker::Builder::useCppSchemaStatementsReader(const cta::catalogue::CatalogueSchema& schema) {
  m_schemaSqlStatementsReader.reset(new CppSchemaStatementsReader(schema));
  return *this;
}

std::unique_ptr<SchemaChecker> SchemaChecker::Builder::build() {
  std::unique_ptr<SchemaChecker> schemaChecker(new SchemaChecker(m_databaseToCheckName,m_dbType,m_catalogueConn));
  if(m_schemaComparer != nullptr){
    schemaChecker->m_schemaComparer = std::move(m_schemaComparer);
    schemaChecker->m_schemaComparer->setSchemaSqlStatementsReader(std::move(m_schemaSqlStatementsReader));
  }
  return schemaChecker;
}

} // namespace cta::catalogue
