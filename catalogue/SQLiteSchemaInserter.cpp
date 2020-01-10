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

#include "SQLiteSchemaInserter.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "DbToSQLiteStatementTransformer.hpp"
#include "common/log/DummyLogger.hpp"
#include <iostream>
#include <fstream>

namespace cta {
namespace catalogue {
  
SQLiteSchemaInserter::SQLiteSchemaInserter(rdbms::Conn &sqliteConn): m_sqliteCatalogueConn(sqliteConn){}

SQLiteSchemaInserter::~SQLiteSchemaInserter() {}

void SQLiteSchemaInserter::insert(const std::list<std::string> &schemaStatements) {
  std::list<std::string> sqliteStatements;
  //Transform the statements in order to make them compatible with the SQLite database 
  for(auto& schemaStatement: schemaStatements){
    std::string sqliteTransformedStatement = DbToSQLiteStatementTransformerFactory::create(schemaStatement)->transform();
    if(!sqliteTransformedStatement.empty()){
      sqliteStatements.emplace_back(sqliteTransformedStatement);
    }
  }
  executeStatements(sqliteStatements);
}

void SQLiteSchemaInserter::executeStatements(const std::list<std::string>& sqliteStatements){
  for(auto &sqliteStatement: sqliteStatements){
    auto stmt = m_sqliteCatalogueConn.createStmt(sqliteStatement);
    stmt.executeNonQuery();
  }
}

}}