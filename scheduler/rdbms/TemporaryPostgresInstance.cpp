/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/TemporaryPostgresInstance.hpp"
#include "common/utils/utils.hpp"

namespace cta::schedulerdb {

/**
 * Execute multiple SQL statements separated by semicolons.
 * This is copied from CreateSchemaCmd::executeNonQueries.
 */
static void executeNonQueries(rdbms::Conn& conn, const std::string& sqlStmts) {
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;

  while (std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
    // Calculate the length of the current statement without the trailing ';'
    const std::string::size_type stmtLen = findResult - searchPos;
    const std::string sqlStmt = utils::trimString(sqlStmts.substr(searchPos, stmtLen));
    searchPos = findResult + 1;

    if (0 < sqlStmt.size()) {  // Ignore empty statements
      conn.executeNonQuery(sqlStmt);
    }
  }
}

/**
  * Create CTA scheduler schema in the test database
  */
void TemporaryPostgresEnvironment::createSchedulerSchema() {
  std::cout << "  Creating CTA scheduler schema..." << std::endl;

  try {
    // Get the schema SQL
    PostgresSchedulerSchema schema;

    // Connect to database and create schema
    auto login = getLogin();
    rdbms::ConnPool connPool(login, 1);
    auto conn = connPool.getConn();

    // Execute schema creation SQL
    executeNonQueries(conn, schema.sql);

    conn.commit();

    std::cout << "  Schema created successfully" << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "  Warning: Failed to create schema: " << e.what() << std::endl;
    // Don't throw - let tests handle missing schema
  }
}

void TemporaryPostgresEnvironment::createSchedulerSchemaInSchema(const std::string& schemaName) {
  try {
    PostgresSchedulerSchema schema;

    auto login = getLogin(schemaName);
    rdbms::ConnPool connPool(login, 1);
    auto conn = connPool.getConn();

    // Set search_path to our schema
    auto setPathStmt = conn.createStmt("SET search_path TO " + std::string("'") + schemaName + std::string("'"));
    setPathStmt.executeNonQuery();

    // Execute schema creation SQL
    executeNonQueries(conn, schema.sql);

    conn.commit();

  } catch (const std::exception& e) {
    throw std::runtime_error("Failed to create scheduler schema in " + schemaName + ": " + e.what());
  }
}

}  // namespace cta::schedulerdb
