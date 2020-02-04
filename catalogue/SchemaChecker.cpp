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

SchemaChecker::Status SchemaChecker::compareSchema(){
    checkSchemaComparerNotNull(__PRETTY_FUNCTION__);
  SchemaComparerResult totalResult;
  std::cout << "Schema version : " << m_databaseMetadataGetter->getCatalogueVersion().getSchemaVersion<std::string>() << std::endl;
  std::cout << "Checking indexes..." << std::endl;
  cta::catalogue::SchemaComparerResult resIndex = m_schemaComparer->compareIndexes();
  totalResult += resIndex;
  resIndex.printDiffs();
  std::cout <<"  "<<resIndex.statusToString(resIndex.getStatus())<<std::endl;
  if(m_dbType == rdbms::Login::DbType::DBTYPE_MYSQL){
    std::cout << "Checking tables and columns..." << std::endl;
  } else {
    std::cout << "Checking tables, columns and constraints..." << std::endl;
  }
  cta::catalogue::SchemaComparerResult resTables = m_schemaComparer->compareTables();
  totalResult += resTables;
  resTables.printDiffs();
  std::cout <<"  "<<resTables.statusToString(resTables.getStatus())<<std::endl;
  std::cout << "Status of the checking : " << cta::catalogue::SchemaComparerResult::statusToString(totalResult.getStatus()) << std::endl;
  if(totalResult.getStatus() == SchemaComparerResult::Status::FAILED){
    return SchemaChecker::FAILURE;
  }
  return SchemaChecker::OK;
}

void SchemaChecker::checkNoParallelTables(){
  std::list<std::string> parallelTables = m_databaseMetadataGetter->getParallelTableNames();
  for(auto& table:parallelTables) {
    std::cout << "WARNING : TABLE " << table << " is set as PARALLEL" << std::endl;
  }
}

void SchemaChecker::checkSchemaNotUpgrading(){
  SchemaVersion catalogueVersion = m_databaseMetadataGetter->getCatalogueVersion();
  if(catalogueVersion.getStatus<SchemaVersion::Status>() == SchemaVersion::Status::UPGRADING){
    std::cout << "WARNING : The status of the schema is " << catalogueVersion.getStatus<std::string>() << ", the future version is " << catalogueVersion.getSchemaVersionNext<std::string>() << std::endl;
  }
}

SchemaChecker::Status SchemaChecker::compareTablesLocatedInSchema(){
  checkSchemaComparerNotNull(__PRETTY_FUNCTION__);
  SchemaComparerResult res = m_schemaComparer->compareTablesLocatedInSchema();
  if(res.getStatus() == SchemaComparerResult::Status::FAILED){
    res.printDiffs();
    return SchemaChecker::Status::FAILURE;
  }
  return SchemaChecker::Status::OK;
}

SchemaChecker::Status SchemaChecker::checkTableContainsColumns(const std::string& tableName, const std::list<std::string> columnNames){
  std::map<std::string, std::string> mapColumnsTypes = m_databaseMetadataGetter->getColumns(tableName);
  SchemaChecker::Status status = SchemaChecker::Status::OK;
  if(mapColumnsTypes.empty()){
    std::cout << "TABLE " << tableName << " does not exist." << std::endl;
    return SchemaChecker::Status::FAILURE;
  }
  for(auto &columnName: columnNames){
    try{
      mapColumnsTypes.at(columnName);
    } catch(std::out_of_range &){
      std::cout << "TABLE " << tableName << " does not contain the column " << columnName << "."<< std::endl;
      status = SchemaChecker::Status::FAILURE;
    }
  }
  return status;
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