/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/schema/PostgresSchedulerSchema.hpp"

#include <iostream>
#include <sstream>

namespace cta::schedulerdb {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresSchedulerSchema::PostgresSchedulerSchema(const std::string& username)
    : SchedulerSchema(PostgresSchedulerSchema::replaceUsername(username)) {}

std::string PostgresSchedulerSchema::replaceUsername(const std::string& username) {
  /* The postgres_scheduler_schema.cpp.in is a file pre-created via CMake
   * as a concatenation of insert_cta_scheduler_version.sql.filled.in
   * and postgres_scheduler_schema.sql. The insert_cta_scheduler_version.sql.filled.in
   * is created by CMake from insert_cta_scheduler_version.sql.in file
   * while filling in the version and schema name
   */
  std::string sql = std::string(  // CTA_SQL_SCHEMA - The contents of postgres_scheduler_schema.cpp.in go here
  );
  // Replace all occurrences of __USERNAME__ with username
  std::string token = "__USERNAME__";
  std::ostringstream out;

  if (!username.empty()) {
    std::size_t pos = 0;
    std::size_t prev = 0;
    while ((pos = sql.find(token, prev)) != std::string::npos) {
      out << sql.substr(prev, pos - prev);
      out << username;
      prev = pos + token.size();
    }
    out << sql.substr(prev);
    return out.str();
  }
  // No username → remove whole SQL statements containing token
  std::size_t pos = 0;
  while (pos < sql.size()) {
    // Find end of current statement
    std::size_t stmtEnd = sql.find(';', pos);
    if (stmtEnd == std::string::npos) {
      stmtEnd = sql.size();
    } else {
      ++stmtEnd;  // include ';'
    }

    const std::string_view statement(sql.data() + pos, stmtEnd - pos);

    // Only append statements that do NOT contain the token
    if (statement.find(token) == std::string_view::npos) {
      out << statement;
    }

    pos = stmtEnd;
  }

  return out.str();
}

}  // namespace cta::schedulerdb
