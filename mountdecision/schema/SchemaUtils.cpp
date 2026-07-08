/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mountdecision/schema/SchemaUtils.hpp"

#include "common/utils/utils.hpp"

#include <algorithm>

namespace cta::mountdecision {

bool tableExists(const std::string& tableName, rdbms::Conn& conn) {
  const auto names = conn.getTableNames();
  return std::any_of(std::begin(names), std::end(names), [&tableName](const auto& name) { return name == tableName; });
}

void executeNonQueries(rdbms::Conn& conn, std::string_view sqlStmts) {
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;

  while (std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
    const std::string::size_type stmtLen = findResult - searchPos;
    const std::string sqlStmt = utils::trimString(sqlStmts.substr(searchPos, stmtLen));
    searchPos = findResult + 1;

    if (!sqlStmt.empty()) {
      conn.executeNonQuery(sqlStmt);
    }
  }
}

}  // namespace cta::mountdecision
