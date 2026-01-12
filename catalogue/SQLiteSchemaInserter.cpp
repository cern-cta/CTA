/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SQLiteSchemaInserter.hpp"

#include "DbToSQLiteStatementTransformer.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/utils/utils.hpp"

#include <fstream>
#include <iostream>

namespace cta::catalogue {

SQLiteSchemaInserter::SQLiteSchemaInserter(rdbms::Conn& sqliteConn) : m_sqliteCatalogueConn(sqliteConn) {}

void SQLiteSchemaInserter::insert(const std::vector<std::string>& schemaStatements) {
  std::vector<std::string> sqliteStatements;
  //Transform the statements in order to make them compatible with the SQLite database
  for (auto& schemaStatement : schemaStatements) {
    std::string sqliteTransformedStatement =
      DbToSQLiteStatementTransformerFactory::create(schemaStatement)->transform();
    if (!sqliteTransformedStatement.empty()) {
      sqliteStatements.emplace_back(sqliteTransformedStatement);
    }
  }
  executeStatements(sqliteStatements);
}

void SQLiteSchemaInserter::executeStatements(const std::vector<std::string>& sqliteStatements) {
  for (auto& sqliteStatement : sqliteStatements) {
    auto stmt = m_sqliteCatalogueConn.createStmt(sqliteStatement);
    stmt.executeNonQuery();
  }
}

}  // namespace cta::catalogue
