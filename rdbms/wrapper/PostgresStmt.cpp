/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/threading/RWLockRdLocker.hpp"

#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"
#include "rdbms/wrapper/PostgresRset.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

#include <algorithm>
#include <exception>
#include <sstream>
#include <utility>
#include <regex>

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresStmt::PostgresStmt(
  PostgresConn &conn,
  const std::string &sql):
  StmtWrapper(sql),
  m_conn(conn),
  m_nParams(0),
  m_nbAffectedRows(0) {

  // connection is rd locked

  CountAndReformatSqlBinds(sql,m_pgsql,m_nParams);

  m_paramValues = std::vector<std::string>(m_nParams);
  m_paramValuesPtrs = std::vector<const char*>(m_nParams);
  m_columnPtrs = std::vector<PostgresColumn*>(m_nParams);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
PostgresStmt::~PostgresStmt() {
  try {
    close(); // Idempotent close() method
  } catch(...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// bindString
//------------------------------------------------------------------------------
void PostgresStmt::bindString(const std::string &paramName, const std::optional<std::string> &paramValue) {
  threading::RWLockWrLocker locker(m_lock);
  try {
    if(paramValue && paramValue.value().empty()) {
      throw exception::Exception(std::string("Optional string parameter ") + paramName + " is an empty string. "
        "An optional string parameter should either have a non-empty string value or no value at all.");
    }

    const unsigned int paramIdx = getParamIdx(paramName); // starts from 1.

    if (paramIdx==0 || paramIdx>m_paramValues.size()) {
      throw exception::Exception(std::string("Bad index for paramName ") + paramName);
    }

    const unsigned int idx = paramIdx - 1;

    if (paramValue) {
      // we must not cause the vector m_paramValues to resize, otherwise the c-pointers can be invalidated
      m_paramValues[idx] = paramValue.value();
      m_paramValuesPtrs[idx] = m_paramValues[idx].c_str();
    } else {
      m_paramValues[idx].clear();
      m_paramValuesPtrs[idx] = nullptr;
    }

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindBool
//------------------------------------------------------------------------------
void bindBool(const std::string &paramName, const std::optional<bool> &paramValue) {
  if(paramValue) {
    bindString(paramName, paramValue.value() ? std::string("t") : std::string("f"));
  } else {
    bindString(paramName, std::nullopt);
  }
}

//------------------------------------------------------------------------------
// bindUint8
//------------------------------------------------------------------------------
void PostgresStmt::bindUint8(const std::string &paramName, const std::optional<uint8_t> &paramValue) {
  try {
    return bindInteger<uint8_t>(paramName, paramValue);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed for SQL statement " +
                        getSqlForException() + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// bindUint16
//------------------------------------------------------------------------------
void PostgresStmt::bindUint16(const std::string &paramName, const std::optional<uint16_t> &paramValue) {
  try {
    return bindInteger<uint16_t>(paramName, paramValue);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed for SQL statement " +
                        getSqlForException() + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// bindUint32
//------------------------------------------------------------------------------
void PostgresStmt::bindUint32(const std::string &paramName, const std::optional<uint32_t> &paramValue) {
  try {
    return bindInteger<uint32_t>(paramName, paramValue);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed for SQL statement " +
                        getSqlForException() + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// bindUint64
//------------------------------------------------------------------------------
void PostgresStmt::bindUint64(const std::string &paramName, const std::optional<uint64_t> &paramValue) {
  try {
    return bindInteger<uint64_t>(paramName, paramValue);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed for SQL statement " +
                        getSqlForException() + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// bindBlob
//------------------------------------------------------------------------------
void PostgresStmt::bindBlob(const std::string &paramName, const std::string &paramValue) {
  /*Escape the bytea string according to https://www.postgresql.org/docs/12/libpq-exec.html*/
  size_t escaped_length;
  auto escapedByteA = PQescapeByteaConn(m_conn.get(), reinterpret_cast<const unsigned char*>(paramValue.c_str()),
    paramValue.length(), &escaped_length);
  std::string escapedParamValue(reinterpret_cast<const char*>(escapedByteA), escaped_length);
  PQfreemem(escapedByteA);
  try {
    bindString(paramName, escapedParamValue);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindDouble
//------------------------------------------------------------------------------
void PostgresStmt::bindDouble(const std::string &paramName, const std::optional<double> &paramValue) {
  threading::RWLockWrLocker locker(m_lock);

  try {
    const unsigned int paramIdx = getParamIdx(paramName); // starts from 1.

    if (paramIdx==0 || paramIdx>m_paramValues.size()) {
      throw exception::Exception(std::string("Bad index for paramName ") + paramName);
    }

    const unsigned int idx = paramIdx - 1;
    if (paramValue) {
      // we must not cause the vector m_paramValues to resize, otherwise the c-pointers can be invalidated
      m_paramValues[idx] = std::to_string(paramValue.value());
      m_paramValuesPtrs[idx] = m_paramValues[idx].c_str();
    } else {
      m_paramValues[idx].clear();
      m_paramValuesPtrs[idx] = nullptr;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void PostgresStmt::clear() {
  threading::RWLockWrLocker locker(m_lock);

  clearAssumeLocked();
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void PostgresStmt::close() {

  // always take statement lock first
  threading::RWLockWrLocker locker2(m_lock);
  threading::RWLockWrLocker locker(m_conn.m_lock);

  closeAssumeLocked();
}

//------------------------------------------------------------------------------
// executeCopyInsert
//------------------------------------------------------------------------------
void PostgresStmt::executeCopyInsert(const size_t rows) {
  // always take statement lock first
  threading::RWLockWrLocker locker2(m_lock);
  threading::RWLockWrLocker locker(m_conn.m_lock);

  try {
    // check connection first
    if (!m_conn.isOpenAssumeLocked()) {
      throw exception::Exception("Connection is closed");
    }

    if (m_conn.isAsyncInProgress()) {
      throw exception::Exception("can not execute sql, another query is in progress");
    }

    for(const auto p: m_columnPtrs) {
      if (nullptr == p) {
        throw exception::Exception("not all columns have been set with setColumn");
      }
      if (p->getNbRows() < rows) {
        std::ostringstream msg;
        msg << "Column " << p->getColName() << " has " << p->getNbRows()
          << " rows, which is less than the requested number " << rows;
        throw exception::Exception(msg.str());
      }
    }

    int nfields=0, binType=0;
    {
      Postgres::Result res(PQexec(m_conn.get(), m_pgsql.c_str()));
      throwDBIfNotStatus(res.get(), PGRES_COPY_IN, "Starting COPY (bulk insert)");
      nfields = PQnfields(res.get());
      binType = PQbinaryTuples(res.get());
    }

    std::ostringstream msg;
    if (nfields != m_nParams) {
      msg << "Wrong number of fields: Copy expected " << nfields << ", we have " << m_nParams;
    } else if (0 != binType) {
      msg << "COPY is expecting binary data, not textual data";
    } else {
      try {
        doCopyData(rows);
      } catch(exception::Exception &ex) {
        msg << "PQputCopyData failed: " << ex.getMessage().str();
      }
    }

    bool err = false;
    std::string pqmsgstr;
    int iret=0;

    if (msg.str().empty()) {
      iret = PQputCopyEnd(m_conn.get(), nullptr);
      doConnectionCheck();
    } else {
      iret = PQputCopyEnd(m_conn.get(), msg.str().c_str());
      err = true;
      pqmsgstr = msg.str();
    }

    if (iret<0 && !err) {
      err = true;
      pqmsgstr = PQerrorMessage(m_conn.get());
    }

    Postgres::ResultItr resItr(m_conn.get());
    // check first result
    resItr.next();
    doConnectionCheck();
    if (!err) {
      if (PGRES_COMMAND_OK != resItr.rcode()) {
        pqmsgstr = PQerrorMessage(m_conn.get());
        err = true;
      }
    }
    if (err) {
      throw exception::Exception(pqmsgstr);
    }

    m_nbAffectedRows = 0;
    const std::string stringValue = PQcmdTuples(resItr.get());
    if (!stringValue.empty()) {
      m_nbAffectedRows = utils::toUint64(stringValue);
    }

  } catch(exception::LostDatabaseConnection &ex) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) +
      " detected lost connection for SQL statement " + getSqlForException() + ": " + ex.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void PostgresStmt::executeNonQuery() {

  // always take statement lock first
  threading::RWLockWrLocker locker2(m_lock);
  threading::RWLockWrLocker locker(m_conn.m_lock);

  try {
    // check connection first
    if (!m_conn.isOpenAssumeLocked()) {
      throw exception::Exception("Connection is closed");
    }

    if (m_conn.isAsyncInProgress()) {
      throw exception::Exception("can not execute sql, another query is in progress");
    }

    if (m_stmt.empty()) {
      doPrepare();
    }

    doPQsendPrepared();
    Postgres::ResultItr resItr(m_conn.get());

    m_nbAffectedRows = 0;
    resItr.next();

    // If this method is used for a query we will get PGRES_TUPLES_OK here
    throwDBIfNotStatus(resItr.get(), PGRES_COMMAND_OK, "Executing non query statement");

    const std::string stringValue = PQcmdTuples(resItr.get());
    if (!stringValue.empty()) {
      m_nbAffectedRows = utils::toUint64(stringValue);
    }

  } catch(exception::LostDatabaseConnection &ex) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) +
      " detected lost connection for SQL statement " + getSqlForException() + ": " + ex.getMessage().str());
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() + ": " +
      ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
std::unique_ptr<RsetWrapper> PostgresStmt::executeQuery() {

  // always take statement lock first
  threading::RWLockWrLocker locker2(m_lock);
  threading::RWLockWrLocker locker(m_conn.m_lock);

  bool isasync = m_conn.isAsyncInProgress();

  try {
    // check connection first
    if (!m_conn.isOpenAssumeLocked()) {
      throw exception::Exception("Connection is closed");
    }

    if (m_conn.isAsyncInProgress()) {
      throw exception::Exception("can not execute sql, another query is in progress");
    }

    if (m_stmt.empty()) {
      doPrepare();
    }

    doPQsendPrepared();
    const int iret = PQsetSingleRowMode(m_conn.get());
    auto resItr = std::make_unique<Postgres::ResultItr>(m_conn.get());

    if (1 != iret) {
      throwDB(nullptr, "Executing query statement");
    }

    m_nbAffectedRows = 0;
    m_conn.setAsyncInProgress(true);

    return std::make_unique<PostgresRset>(m_conn, *this, std::move(resItr));
  } catch(exception::LostDatabaseConnection &ex) {
    // reset to initial value
    m_conn.setAsyncInProgress(isasync);
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) +
      " detected lost connection for SQL statement " + getSqlForException() + ": " + ex.getMessage().str());
  } catch(exception::Exception &ex) {
    // reset to initial value
    m_conn.setAsyncInProgress(isasync);
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  } catch(std::exception &) {
    // reset to initial value
    m_conn.setAsyncInProgress(isasync);
    throw;
  }
}

//------------------------------------------------------------------------------
// getNbAffectedRows
//------------------------------------------------------------------------------
uint64_t PostgresStmt::getNbAffectedRows() const {
  threading::RWLockRdLocker locker(m_lock);
  return m_nbAffectedRows;
}

//------------------------------------------------------------------------------
// setColumn
//------------------------------------------------------------------------------
void PostgresStmt::setColumn(PostgresColumn &col) {
  threading::RWLockWrLocker locker(m_lock);
  try {
    const std::string paramName = std::string(":") + col.getColName();
    const auto paramIdx = getParamIdx(paramName);
    if (paramIdx==0 || paramIdx>m_columnPtrs.size()) {
      throw exception::Exception(std::string("Bad index for paramName ") + paramName);
    }
    const unsigned int idx = paramIdx - 1;
    m_columnPtrs[idx] = &col;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// clearAssumeLocked
//------------------------------------------------------------------------------
void PostgresStmt::clearAssumeLocked() {
  // assumes statement lock is taken rw

  m_paramValues = std::vector<std::string>(m_nParams);
  m_paramValuesPtrs = std::vector<const char*>(m_nParams);
  m_columnPtrs = std::vector<PostgresColumn*>(m_nParams);
}

//------------------------------------------------------------------------------
// closeAssumeLocked
//------------------------------------------------------------------------------
void PostgresStmt::closeAssumeLocked() {

  // assumes both statement and connection locks held rw

  if (m_stmt.empty()) {
    return;
  }

  try {
    clearAssumeLocked();
    const std::string stmt = m_stmt;
    m_stmt.clear();
    m_conn.deallocateStmt(stmt);

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// closeBoth
//------------------------------------------------------------------------------
void PostgresStmt::closeBoth() {
  // assumes both statement and connection locks held
  try {
    closeAssumeLocked();
  } catch(std::exception &) {
    // nothing
  }
  try {
    m_conn.closeAssumeLocked();
  } catch(std::exception &) {
    // nothing
  }
}

//------------------------------------------------------------------------------
// CountAndReformatSqlBinds
//------------------------------------------------------------------------------
void PostgresStmt::CountAndReformatSqlBinds(const std::string &common_sql, std::string &pg_sql, int &nParams) const {
  nParams = 0;
  // Match any :name
  std::regex pattern(R"(:(\w+))");
  std::smatch match;
  std::string::const_iterator searchStart(common_sql.cbegin());
  std::string result;
  std::ostringstream oss;
  while (std::regex_search(searchStart, common_sql.cend(), match, pattern)) {
    // skip all matches which have a second colon in front e.g. ::name (reserved for type casting in postgres)
    auto matchPos = std::distance(common_sql.cbegin(), searchStart) + match.position();
    if (matchPos > 0 && ':' == *(common_sql.cbegin() + matchPos - 1)){
      oss << match.prefix();
      oss << match.str();
      searchStart = match.suffix().first;
      continue;
    }
    ++nParams; // Increment the parameter counter
    oss << match.prefix();
    oss << "$" << nParams;
    searchStart = match.suffix().first;
  }
  // Append the remaining part of the string after the last match
  oss << std::string(searchStart, common_sql.cend());
  pg_sql = oss.str();
}

//------------------------------------------------------------------------------
// doPQsendPrepared
//------------------------------------------------------------------------------
void PostgresStmt::doPQsendPrepared() {
  // assumes the connection and statement locks have been taken

  const char **params = nullptr;
  if (m_nParams>0) {
    params = &m_paramValuesPtrs[0];
  }

  const int iret = PQsendQueryPrepared(m_conn.get(), m_stmt.c_str(), m_nParams, params, nullptr, nullptr, 0);
  if (1 != iret) {
    throwDB(nullptr, "Executing a prepared statement");
  }
}

//------------------------------------------------------------------------------
// doConnectionCheck
//------------------------------------------------------------------------------
void PostgresStmt::doConnectionCheck() {
  // assumes both statement and connection are locked
  if ( !m_conn.isOpenAssumeLocked() ) {
    closeBoth();
    throw exception::LostDatabaseConnection("Database connection has been lost");
  }
}

//------------------------------------------------------------------------------
// doCopyData
//------------------------------------------------------------------------------
void PostgresStmt::doCopyData(const size_t rows) {
  for(size_t i=0;i<rows;++i) {
    m_copyRow.clear();
    for(int j=0;j<m_nParams;++j) {
      const char *const val = m_columnPtrs[j]->getValue(i);
      std::string field;
      if (nullptr != val) {
        field = val;
        replaceAll(field, "\\", "\\\\");
        replaceAll(field, "\n", "\\\n");
        replaceAll(field, "\r", "\\\r");
        replaceAll(field, "\t", "\\\t");
      } else {
        field = "\\N";
      }
      if (!m_copyRow.empty()) {
        m_copyRow += "\t";
      }
      m_copyRow += field;
    }
    m_copyRow += "\n";
    const int iret = PQputCopyData(m_conn.get(), m_copyRow.c_str(), m_copyRow.size());
    if (iret < 0) {
      throwDB(nullptr, "Writing bulk insert data to the database");
    }
  }
}

//------------------------------------------------------------------------------
// doPrepare
//------------------------------------------------------------------------------
void PostgresStmt::doPrepare() {
  // assumes the connection object is aleady rw locked, and open and not in async

  const std::string stmtName = m_conn.nextStmtName();

  Postgres::Result res(PQprepare(m_conn.get(), stmtName.c_str(), m_pgsql.c_str(), m_nParams, nullptr));
  throwDBIfNotStatus(res.get(), PGRES_COMMAND_OK, "Preparing a statement");

  m_stmt = stmtName;
}

//------------------------------------------------------------------------------
// replaceAll
//------------------------------------------------------------------------------
void PostgresStmt::replaceAll(std::string& str, const std::string& from, const std::string& to) const {
  if(from.empty()) {
    return;
  }
  size_t start_pos = 0;
  while((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length(); // In case to contains from
  }
}

//------------------------------------------------------------------------------
// throwDB
//------------------------------------------------------------------------------
void PostgresStmt::throwDB(const PGresult *res, const std::string &prefix) {
  // assums both statement and connection lock held
  try {
    Postgres::ThrowInfo(m_conn.get(),res,prefix);
  } catch(exception::LostDatabaseConnection &) {
    closeBoth();
    throw;
  }
}

//------------------------------------------------------------------------------
// throwDBIfNotStatus
//------------------------------------------------------------------------------
void PostgresStmt::throwDBIfNotStatus(const PGresult *res,
          const ExecStatusType requiredStatus, const std::string &prefix) {
  if (PQresultStatus(res) != requiredStatus) {
    throwDB(res, prefix);
  }
}

} // namespace cta::rdbms::wrapper
