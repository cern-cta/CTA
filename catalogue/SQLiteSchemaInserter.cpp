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

#include "SQLiteSchemaInserter.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "DbToSQLiteStatementTransformer.hpp"
#include "common/log/DummyLogger.hpp"
#include <iostream>
#include <fstream>

namespace cta::catalogue {

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

} // namespace cta::catalogue
