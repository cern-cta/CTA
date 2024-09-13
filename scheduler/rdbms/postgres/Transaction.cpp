/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright	   Copyright © 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "scheduler/rdbms/postgres/Transaction.hpp"

namespace cta::schedulerdb {

Transaction::Transaction(std::unique_ptr<cta::rdbms::Conn> conn, bool ownConnection)
        : m_conn(std::move(conn)), m_ownConnection(ownConnection) {
  start();
}

Transaction::Transaction(cta::rdbms::ConnPool& connPool)
        : m_conn(std::make_shared<cta::rdbms::Conn>(connPool.getConn())), m_ownConnection(true) {
  start();
}

Transaction::Transaction(Transaction&& other) noexcept
: m_conn(std::move(other.m_conn)), m_ownConnection(other.m_ownConnection) {
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
  }
  // Only delete the connection if we own it
  if (m_ownConnection) {
    m_conn.reset();  // Release the connection
  }
}

void Transaction::lockGlobal() {
  // a global access lock to the whole database
  // for anyone trying to acquire the same lock
  std::string sql = "SELECT PG_ADVISORY_XACT_LOCK(:LOCK_ID::bigint)";
  auto stmt = m_conn->createStmt(sql);
  stmt.bindUint64(":LOCK_ID",0);
  stmt.executeQuery();
}

void Transaction::takeNamedLock(std::string_view tapePoolString) {
  std::hash<std::string_view> lock_id_hasher;
  std::size_t lock_id = lock_id_hasher(tapePoolString);
  // Convert to 64-bit integer
  uint64_t hash64 = static_cast<uint64_t>(lock_id);
  uint32_t hash32 = static_cast<uint32_t>(hash64 ^ (hash64 >> 32));

  //std::cout << "Hash value (64-bit): " << hash64 << std::endl;
  std::string sql = "SELECT PG_ADVISORY_XACT_LOCK(:HASH32::bigint)";
  auto stmt = m_conn->createStmt(sql);
  stmt.bindUint64(":HASH32",hash32);
  stmt.executeQuery();
}

void Transaction::resetConn(cta::rdbms::ConnPool& connPool) {
  if (m_ownConnection) {
    // Reset the old connection only if we own it
    m_conn.reset();
  }
  // Obtain a new connection from the pool
  m_conn = std::make_shared<cta::rdbms::Conn>(connPool.getConn());
  m_ownConnection = true;
}

std::unique_ptr<cta::rdbms::Conn> Transaction::getConn() const {
  return m_conn;
}

void Transaction::start() {
  if (!m_begin) {
    m_conn->executeNonQuery(R"SQL(BEGIN)SQL");
    m_begin = true;
  }
}

void Transaction::start_insert() {
  if (!m_begin) {
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

} // namespace cta::schedulerdb
