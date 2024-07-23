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

Transaction::Transaction(rdbms::ConnPool &connPool) :
  m_conn(connPool.getConn())
{
  start();
}

Transaction::~Transaction() { 
  if (m_begin) {
    m_conn.rollback();
  }
}

void Transaction::lockGlobal(uint64_t lockId) {
  std::string sql = "SELECT PG_ADVISORY_XACT_LOCK(" + std::to_string(lockId) + ")";
  auto stmt = m_conn.createStmt(sql);
  stmt.executeQuery();
}

void Transaction::start() {
  if (!m_begin) {
    m_conn.executeNonQuery(R"SQL(BEGIN)SQL");
    m_begin = true;
  }
}

void Transaction::commit() {
  m_conn.commit();
  m_begin = false;
}

void Transaction::abort() {
  m_conn.rollback();
  m_begin = false;
}

rdbms::Conn &Transaction::getNonTxnConn() {
  if (!m_begin) {
    return m_conn;
  } else {
    commit();
    return m_conn;
  }
}

rdbms::Conn &Transaction::conn() {
  if (!m_begin) {
    throw SQLError(std::string("No transaction"));
  }
  return m_conn;
}
} // namespace cta::schedulerdb
