/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "SchemaChecker.hpp"
#include "SQLiteSchemaComparer.hpp"

namespace cta{
namespace catalogue{
  
SchemaChecker::SchemaChecker(const std::string databaseToCheckName, rdbms::Login::DbType dbType,cta::rdbms::Conn &conn):m_databaseToCheckName(databaseToCheckName),m_dbType(dbType),m_catalogueConn(conn) {
  m_databaseMetadataGetter.reset(DatabaseMetadataGetterFactory::create(m_dbType,m_catalogueConn)); 
}

SchemaChecker::~SchemaChecker() {
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
  if(m_dbType == rdbms::Login::DbType::DBTYPE_MYSQL){
    stdOut << "Checking tables and columns..." << std::endl;
  } else {
    stdOut << "Checking tables, columns and constraints..." << std::endl;
  }
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

SchemaCheckerResult SchemaChecker::checkTableContainsColumns(const std::string& tableName, const std::list<std::string> columnNames){
  std::map<std::string, std::string> mapColumnsTypes = m_databaseMetadataGetter->getColumns(tableName);
  SchemaCheckerResult res;
  if(mapColumnsTypes.empty()){
    std::string error = "TABLE " + tableName +" does not exist.";
    res.addError(error);
    return res;
  }
  for(auto &columnName: columnNames){
    try{
      mapColumnsTypes.at(columnName);
    } catch(std::out_of_range &){
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

/////////////////////////////////////////
// SchemaChecker::Builder
/////////////////////////////////////////
SchemaChecker::Builder::Builder(const std::string databaseToCheckName, const cta::rdbms::Login::DbType dbType, cta::rdbms::Conn & conn):m_databaseToCheckName(databaseToCheckName), m_dbType(dbType),m_catalogueConn(conn){
  m_databaseMetadataGetter.reset(DatabaseMetadataGetterFactory::create(m_dbType,m_catalogueConn)); 
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

SchemaChecker::Builder& SchemaChecker::Builder::useCppSchemaStatementsReader(const cta::catalogue::CatalogueSchema schema){
  m_schemaSqlStatementsReader.reset(new CppSchemaStatementsReader(schema));
  return *this;
}

std::unique_ptr<SchemaChecker> SchemaChecker::Builder::build() {
  std::unique_ptr<SchemaChecker> schemaChecker(new SchemaChecker(m_databaseToCheckName,m_dbType,m_catalogueConn));
  if(m_schemaComparer != nullptr){
    schemaChecker->m_schemaComparer = std::move(m_schemaComparer);
    schemaChecker->m_schemaComparer->setSchemaSqlStatementsReader(std::move(m_schemaSqlStatementsReader));
  }
  return std::move(schemaChecker);
}

}}