/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/TemporaryPostgresInstance.hpp"

namespace cta::schedulerdb {

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

/**
 * Execute multiple SQL statements separated by semicolons
 */
void TemporaryPostgresEnvironment::executeNonQueries(rdbms::Conn& conn, const std::string& sqlStmts) {
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;

  std::cout << "In TemporaryPostgresEnvironment::executeNonQueries" << std::endl;

  while (std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
    const std::string sqlStmt = sqlStmts.substr(searchPos, findResult - searchPos);

    // Only execute non-empty statements
    if (!sqlStmt.empty() && sqlStmt.find_first_not_of(" \t\n\r") != std::string::npos) {
      auto stmt = conn.createStmt(sqlStmt);
      stmt.executeNonQuery();
    }

    searchPos = findResult + 1;
  }
}

}  // namespace cta::schedulerdb
