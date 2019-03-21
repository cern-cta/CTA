/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/PostgresRset.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"
#include "common/threading/RWLockWrLocker.hpp"

#include <utility>

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresRset::PostgresRset(PostgresConn &conn, PostgresStmt &stmt, std::unique_ptr<Postgres::ResultItr> resItr)
  : m_conn(conn), m_stmt(stmt), m_resItr(std::move(resItr)), m_asyncCleared(false), m_nfetched(0) {

  // assumes statement and connection locks have already been taken
  if (!m_conn.isAsyncInProgress()) {
    throw exception::Exception(std::string(__FUNCTION__) + " unexpected: async flag not set");
  }
}

//------------------------------------------------------------------------------
// destructor.
//------------------------------------------------------------------------------
PostgresRset::~PostgresRset() {

  try {
    threading::RWLockWrLocker locker(m_conn.m_lock);

    m_resItr->clear();
    doClearAsync();
  } catch(...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// columnIsNull
//------------------------------------------------------------------------------
bool PostgresRset::columnIsNull(const std::string &colName) const {

  if (nullptr == m_resItr->get()) {
    throw exception::Exception(std::string(__FUNCTION__) + " no row available");
  }

  const int ifield = PQfnumber(m_resItr->get(), colName.c_str());
  if (ifield < 0) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
  }

  return PQgetisnull(m_resItr->get(), 0, ifield);
}

//------------------------------------------------------------------------------
// columnOptionalString
//------------------------------------------------------------------------------
optional<std::string> PostgresRset::columnOptionalString(const std::string &colName) const {

  if (nullptr == m_resItr->get()) {
    throw exception::Exception(std::string(__FUNCTION__) + " no row available");
  }

  const int ifield = PQfnumber(m_resItr->get(), colName.c_str());
  if (ifield < 0) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
  }

  // the value can be null
  if (PQgetisnull(m_resItr->get(), 0, ifield)) {
    return nullopt;
  }

  return optional<std::string>(PQgetvalue(m_resItr->get(), 0, ifield));
}

//------------------------------------------------------------------------------
// columnOptionalUint64
//------------------------------------------------------------------------------
optional<uint64_t> PostgresRset::columnOptionalUint64(const std::string &colName) const {

  if (nullptr == m_resItr->get()) {
    throw exception::Exception(std::string(__FUNCTION__) + " no row available");
  }

  const int ifield = PQfnumber(m_resItr->get(), colName.c_str());
  if (ifield < 0) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
  }

  // the value can be null
  if (PQgetisnull(m_resItr->get(), 0, ifield)) {
    return nullopt;
  }

  const std::string stringValue(PQgetvalue(m_resItr->get(), 0, ifield));

  if(!utils::isValidUInt(stringValue)) {
    throw exception::Exception(std::string("Column ") + colName + " contains the value " + stringValue +
      " which is not a valid unsigned integer");
  }

  return utils::toUint64(stringValue);
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &PostgresRset::getSql() const {
  return m_stmt.getSql();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool PostgresRset::next() {

  // always locks in order statement and then connection
  threading::RWLockWrLocker locker2(m_stmt.m_lock);
  threading::RWLockWrLocker locker(m_conn.m_lock);

  if (m_resItr->next()) {

    // For queries expect rcode PGRES_SINGLE_TUPLE with ntuples=1 for each row,
    // followed by PGRES_TUPLES_OK and ntuples=0 at the end.
    // A non query would give PGRES_COMMAND_OK but we don't accept this here
    // as a Rset is intended for an executeQuery only.

    if (PGRES_TUPLES_OK == m_resItr->rcode() && 0 == PQntuples(m_resItr->get())) {
      const std::string stringValue = PQcmdTuples(m_resItr->get());
      if (!stringValue.empty()) {
        m_stmt.setAffectedRows(utils::toUint64(stringValue));
      }
      m_resItr->clear();
      doClearAsync();
      return false;
    }
    if (PGRES_SINGLE_TUPLE == m_resItr->rcode() && 1 == PQntuples(m_resItr->get())) {
      ++m_nfetched;
      m_stmt.setAffectedRows(m_nfetched);
      return true;
    }
    m_resItr->clear();
    doClearAsync();
    m_stmt.throwDB(m_resItr->get(), std::string(__FUNCTION__) + " failed while fetching results");
  }
  doClearAsync();
  m_stmt.doConnectionCheck();
  return false;
}

//------------------------------------------------------------------------------
// doClearAsync
//------------------------------------------------------------------------------
void PostgresRset::doClearAsync() {
  // assumes we have lock on connection
  if (!m_asyncCleared) {
    m_conn.setAsyncInProgress(false);
    m_asyncCleared = true;
  }
}


} // namespace wrapper
} // namespace rdbms
} // namespace cta
