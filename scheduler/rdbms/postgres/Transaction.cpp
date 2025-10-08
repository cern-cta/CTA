/**
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/postgres/Transaction.hpp"

namespace cta::schedulerdb {

Transaction::Transaction(std::unique_ptr<cta::rdbms::Conn> conn, bool ownConnection)
    : m_conn(std::move(conn)),
      m_ownConnection(ownConnection) {
  start();
}

Transaction::Transaction(cta::rdbms::ConnPool& connPool)
    : m_conn(std::make_unique<cta::rdbms::Conn>(connPool.getConn())),
      m_ownConnection(true) {
  start();
}

Transaction::Transaction(Transaction&& other) noexcept
    : m_conn(std::move(other.m_conn)),
      m_ownConnection(other.m_ownConnection) {
  start();
}

Transaction& Transaction::operator=(Transaction&& other) noexcept {
  if (this != &other) {
    m_conn = std::move(other.m_conn);
    m_ownConnection = other.m_ownConnection;
  }
  return *this;
}

Transaction::~Transaction() {
  if (m_begin) {
    m_conn->rollback();
    m_conn.reset();
  }
  m_conn.reset();  // Release the connection
}

void Transaction::lockGlobal() {
  // a global access lock to the whole database
  // for anyone trying to acquire the same lock
  std::string sql = "SELECT PG_ADVISORY_XACT_LOCK(:LOCK_ID::bigint)";
  auto stmt = m_conn->createStmt(sql);
  stmt.bindUint64(":LOCK_ID", 0);
  stmt.executeQuery();
}

void Transaction::takeNamedLock(std::string_view tapePoolString) {
  std::hash<std::string_view> lock_id_hasher;
  std::size_t lock_id = lock_id_hasher(tapePoolString);
  // Convert to 64-bit integer
  auto hash64 = static_cast<uint64_t>(lock_id);
  auto hash32 = static_cast<uint32_t>(hash64 ^ (hash64 >> 32));

  // debug: std::cout << "Hash value (64-bit): " << hash64 << std::endl;
  std::string sql = "SELECT PG_ADVISORY_XACT_LOCK(:HASH32::bigint)";
  auto stmt = m_conn->createStmt(sql);
  stmt.bindUint64(":HASH32", hash32);
  stmt.executeQuery();
}

void Transaction::resetConn(cta::rdbms::ConnPool& connPool) {
  // Reset the old connection only if we own it
  m_conn.reset();
  // Obtain a new connection from the pool
  m_conn = std::make_unique<cta::rdbms::Conn>(connPool.getConn());
  m_ownConnection = true;
}

cta::rdbms::Conn& Transaction::getConn() const {
  return *m_conn;
}

void Transaction::start() {
  if (!m_begin) {
    m_conn->executeNonQuery(R"SQL(BEGIN)SQL");
    m_begin = true;
  }
}

void Transaction::commit() {
  m_conn->commit();
  m_begin = false;
}

void Transaction::abort() {
  m_conn->rollback();
  m_begin = false;
}

}  // namespace cta::schedulerdb
