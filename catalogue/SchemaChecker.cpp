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
  
SchemaChecker::SchemaChecker(rdbms::Login::DbType dbType,cta::rdbms::Conn &conn):m_dbType(dbType),m_catalogueConn(conn) {
}

SchemaChecker::~SchemaChecker() {
}

void SchemaChecker::useSQLiteSchemaComparer(const cta::optional<std::string> allSchemasVersionsDirectory){
  m_schemaComparer.reset(new SQLiteSchemaComparer(m_dbType,m_catalogueConn));
   std::unique_ptr<SchemaSqlStatementsReader> schemaSqlStatementsReader;
  if(allSchemasVersionsDirectory){
    schemaSqlStatementsReader.reset(new DirectoryVersionsSqlStatementsReader(m_dbType,m_schemaComparer->getCatalogueVersion(),allSchemasVersionsDirectory.value()));
  } else {
    schemaSqlStatementsReader.reset(new MapSqlStatementsReader(m_dbType,m_schemaComparer->getCatalogueVersion()));
  }
  m_schemaComparer->setSchemaSqlStatementsReader(std::move(schemaSqlStatementsReader));
}

SchemaChecker::Status SchemaChecker::compareSchema(){
  if(m_schemaComparer == nullptr){
    throw cta::exception::Exception("No schema comparer used. Please specify the schema comparer by using the methods useXXXXSchemaComparer()");
  }
  SchemaComparerResult totalResult;
  std::cout << "Schema version : " << m_schemaComparer->getCatalogueVersion() << std::endl;
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
  std::list<std::string> parallelTables = m_catalogueConn.getParallelTableNames();
  for(auto& table:parallelTables) {
    std::cout << "WARNING : TABLE " << table << " is set as PARALLEL" << std::endl;
  }
}

}}