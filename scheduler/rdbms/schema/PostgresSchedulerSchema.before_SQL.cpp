/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/schema/PostgresSchedulerSchema.hpp"

namespace cta::schedulerdb {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresSchedulerSchema::PostgresSchedulerSchema(const std::string& username)
    : SchedulerSchema(PostgresSchedulerSchema::replaceUsername(username)) {}

std::string PostgresSchedulerSchema::replaceUsername(const std::string& username) {
  std::string sql = std::string(  // CTA_SQL_SCHEMA - The contents of postgres_scheduler_schema.cpp go here
  );
  // Replace all occurrences of __USERNAME__ with username
  std::string token = "__USERNAME__";
  size_t pos = 0;
  while ((pos = sql.find(token, pos)) != std::string::npos) {
    sql.replace(pos, token.length(), username);
    pos += username.length();  // move past the replacement
  }

  return sql;
}

}  // namespace cta::schedulerdb
