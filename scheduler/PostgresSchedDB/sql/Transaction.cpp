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

#include "scheduler/PostgresSchedDB/sql/Transaction.hpp"

namespace cta::postgresscheddb {

Transaction::Transaction(rdbms::ConnPool &connPool) :
  m_conn(std::move(connPool.getConn())), m_begin(false)
{
  m_conn.executeNonQuery("BEGIN");
  m_begin = true;
}

Transaction::Transaction(Transaction &&other):
  m_conn(std::move(other.m_conn)), m_begin(other.m_begin) { }

Transaction::~Transaction() { 
  if (m_begin) {
    m_conn.rollback();
  }
}

Transaction &Transaction::operator=(Transaction &&rhs) {
  m_conn = std::move(rhs.m_conn);
  m_begin = rhs.m_begin;
  return *this;
}

void Transaction::lockGlobal(uint64_t lockId) {
  const std::string sql = "SELECT PG_ADVISORY_XACT_LOCK(" + std::to_string(lockId) + ")";
  auto stmt = m_conn.createStmt(sql);
  stmt.executeQuery();
}

void Transaction::commit() {
  m_conn.commit();
  m_begin = false;
}

void Transaction::abort() {
  m_conn.rollback();
  m_begin = false;
}

rdbms::Conn &Transaction::conn() {
  if (!m_begin) {
    throw SQLError(std::string("No transaction"));
  }
  return m_conn;
}

} // namespace cta::postgresscheddb
