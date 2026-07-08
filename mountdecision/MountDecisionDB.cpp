/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mountdecision/MountDecisionDB.hpp"

#include "common/exception/Exception.hpp"

namespace cta::mountdecision {

MountDecisionDB::MountDecisionDB(const std::string& ownerId, log::Logger&, const rdbms::Login& login, uint64_t nbConns)
    : m_ownerId(ownerId),
      m_connPool(login, nbConns) {}

void MountDecisionDB::ping() {
  auto conn = m_connPool.getConn();
  const auto tableNames = conn.getTableNames();
  for (const auto& tableName : tableNames) {
    if (tableName == "MOUNT_DECISION_KV") {
      return;
    }
  }
  throw exception::Exception("Did not find MOUNT_DECISION_KV table in the Mount Decision DB.");
}

void MountDecisionDB::setValue(const std::string& key, const std::string& value) {
  auto conn = m_connPool.getConn();
  const char* const sql = R"SQL(
    INSERT INTO MOUNT_DECISION_KV(
      KEY_NAME,
      VALUE
    ) VALUES (
      :KEY_NAME,
      :VALUE
    )
    ON CONFLICT(KEY_NAME) DO UPDATE SET
      VALUE = EXCLUDED.VALUE
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":KEY_NAME", key);
  stmt.bindString(":VALUE", value);
  stmt.executeNonQuery();
}

std::optional<std::string> MountDecisionDB::getValue(const std::string& key) {
  auto conn = m_connPool.getConn();
  const char* const sql = R"SQL(
    SELECT
      VALUE AS VALUE
    FROM
      MOUNT_DECISION_KV
    WHERE
      KEY_NAME = :KEY_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":KEY_NAME", key);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    return std::nullopt;
  }
  return rset.columnString("VALUE");
}

void MountDecisionDB::incrementCounter(const std::string& key) {
  auto conn = m_connPool.getConn();
  const char* const sql = R"SQL(
    INSERT INTO MOUNT_DECISION_KV(
      KEY_NAME,
      VALUE
    ) VALUES (
      :KEY_NAME,
      '1'
    )
    ON CONFLICT(KEY_NAME) DO UPDATE SET
      VALUE = CAST((CAST(MOUNT_DECISION_KV.VALUE AS BIGINT) + 1) AS VARCHAR(255))
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":KEY_NAME", key);
  stmt.executeNonQuery();
}

}  // namespace cta::mountdecision
