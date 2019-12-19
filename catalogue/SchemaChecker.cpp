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

void SchemaChecker::useSQLiteSchemaComparer(const std::string &allSchemasVersionPath){
  m_schemaComparer.reset(new SQLiteSchemaComparer(m_dbType,m_catalogueConn,allSchemasVersionPath));
}

SchemaChecker::Status SchemaChecker::compareSchema(){
  if(m_schemaComparer == nullptr){
    throw cta::exception::Exception("No schema comparer used. Please specify the schema comparer by using the methods useXXXXSchemaComparer(schemaComparer)");
  }
  cta::catalogue::SchemaComparerResult res = m_schemaComparer->compare();
  std::cout << "Schema version : " << m_schemaComparer->getCatalogueVersion() << std::endl;
  res.printDiffs();
  std::cout << "Status of the checking : " << cta::catalogue::SchemaComparerResult::StatusToString(res.getStatus()) << std::endl;
  if(res.getStatus() == SchemaComparerResult::Status::FAILED){
    return SchemaChecker::FAILURE;
  }
  return SchemaChecker::OK;
}

void SchemaChecker::checkNoParallelTables(){
  
}

}}