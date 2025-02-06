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
#include "rdbms/NullDbValue.hpp"
#include "rdbms/wrapper/PostgresRset.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"
#include "common/threading/RWLockWrLocker.hpp"
#include <cctype>

#include <utility>

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresRset::PostgresRset(PostgresConn& conn, PostgresStmt& stmt, std::unique_ptr<Postgres::ResultItr> resItr)
    : m_conn(conn),
      m_stmt(stmt),
      m_resItr(std::move(resItr)),
      m_asyncCleared(false),
      m_nfetched(0) {
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
  } catch (...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// getting column index using a local cache to avoid looking up index
// for every column for each row of the Rset whenever we loop over the result
//------------------------------------------------------------------------------
int PostgresRset::getColumnIndex(const std::string& colName) const {
  if (nullptr == m_resItr->get()) {
    throw exception::Exception(std::string(__FUNCTION__) + " no row available");
  }
  auto it = m_columnPQindexCache.find(colName);
  if (it != m_columnPQindexCache.end()) {
    return it->second;
  }
  int idx = PQfnumber(m_resItr->get(), colName.c_str());
  if (idx < 0) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
  }
  m_columnPQindexCache[colName] = idx;
  return idx;
}

//------------------------------------------------------------------------------
// columnIsNull
//------------------------------------------------------------------------------
bool PostgresRset::columnIsNull(const std::string& colName) const {
  const int ifield = getColumnIndex(colName);
  return PQgetisnull(m_resItr->get(), 0, ifield);
}

//------------------------------------------------------------------------------
// isPGColumnNull
//------------------------------------------------------------------------------
bool PostgresRset::isPGColumnNull(int ifield) const {
  return PQgetisnull(m_resItr->get(), 0, ifield);
}

std::string PostgresRset::columnBlob(const std::string& colName) const {
  std::optional<std::string> blob = columnOptionalString(colName);

  if (blob) {
    size_t blob_len;
    unsigned char* blob_ptr = PQunescapeBytea(reinterpret_cast<const unsigned char*>(blob->c_str()), &blob_len);

    if (blob_ptr != nullptr) {
      // using unique_ptr with custom deleter to automatically free memory
      std::unique_ptr<unsigned char, decltype(&PQfreemem)> blob_ptr_guard(blob_ptr, &PQfreemem);
      // using move semantics to avoid unnecessary copies
      return std::string(reinterpret_cast<const char*>(blob_ptr_guard.get()), blob_len);
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } else {
    throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
  }
}

bool PostgresRset::columnBoolNoOpt(const std::string& colName) const {
  try {
    const auto strValue = columnStringNoOpt(colName);
    if (strValue == "t" || strValue == "true") {
      return true;
    } else if (strValue == "f" || strValue == "false") {
      return false;
    } else {
      throw exception::Exception("Invalid boolean string representation: " + strValue);
    }
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

std::string PostgresRset::columnStringNoOpt(const std::string& colName) const {
  try {
    const int ifield = getColumnIndex(colName);

    if (isPGColumnNull(ifield)) {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }

    const char* cstrValue = PQgetvalue(m_resItr->get(), 0, ifield);

    return std::string(cstrValue, PQgetlength(m_resItr->get(), 0, ifield));
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// Get uint8_t value from a column with error handling
//------------------------------------------------------------------------------
uint8_t PostgresRset::columnUint8NoOpt(const std::string& colName) const {
  try {
    const int ifield = getColumnIndex(colName);

    if (isPGColumnNull(ifield)) {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }

    const char* cstrValue = PQgetvalue(m_resItr->get(), 0, ifield);
    return utils::toPGUint8(std::string_view(cstrValue, PQgetlength(m_resItr->get(), 0, ifield)));
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// Get uint16_t value from a column with error handling
//------------------------------------------------------------------------------
uint16_t PostgresRset::columnUint16NoOpt(const std::string& colName) const {
  try {
    const int ifield = getColumnIndex(colName);

    if (isPGColumnNull(ifield)) {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }

    const char* cstrValue = PQgetvalue(m_resItr->get(), 0, ifield);

    return utils::toPGUint16(std::string_view(cstrValue, PQgetlength(m_resItr->get(), 0, ifield)));
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// Get uint32_t value from a column with error handling
//------------------------------------------------------------------------------
uint32_t PostgresRset::columnUint32NoOpt(const std::string& colName) const {
  try {
    const int ifield = getColumnIndex(colName);

    if (isPGColumnNull(ifield)) {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }

    const char* cstrValue = PQgetvalue(m_resItr->get(), 0, ifield);

    return utils::toPGUint32(std::string_view(cstrValue, PQgetlength(m_resItr->get(), 0, ifield)));
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// Get uint64_t value from a column with error handling
//------------------------------------------------------------------------------
uint64_t PostgresRset::columnUint64NoOpt(const std::string& colName) const {
  try {
    const int ifield = getColumnIndex(colName);

    if (isPGColumnNull(ifield)) {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }

    const char* cstrValue = PQgetvalue(m_resItr->get(), 0, ifield);

    return utils::toPGUint64(std::string_view(cstrValue, PQgetlength(m_resItr->get(), 0, ifield)));
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// Get double value from a column with error handling
//------------------------------------------------------------------------------
double PostgresRset::columnDoubleNoOpt(const std::string& colName) const {
  try {
    const int ifield = getColumnIndex(colName);

    if (isPGColumnNull(ifield)) {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }

    const char* cstrValue = PQgetvalue(m_resItr->get(), 0, ifield);

    return utils::toPGDouble(std::string_view(cstrValue, PQgetlength(m_resItr->get(), 0, ifield)));
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalString
//------------------------------------------------------------------------------
std::optional<std::string> PostgresRset::columnOptionalString(const std::string& colName) const {
  const int ifield = getColumnIndex(colName);

  // the value can be null
  if (PQgetisnull(m_resItr->get(), 0, ifield)) {
    return std::nullopt;
  }

  return std::optional<std::string>(std::move(PQgetvalue(m_resItr->get(), 0, ifield)));
}

//------------------------------------------------------------------------------
// columnOptionalUint8
//------------------------------------------------------------------------------
std::optional<uint8_t> PostgresRset::columnOptionalUint8(const std::string& colName) const {
  const int ifield = getColumnIndex(colName);

  // the value can be null
  if (PQgetisnull(m_resItr->get(), 0, ifield)) {
    return std::nullopt;
  }

  return utils::toUint8(PQgetvalue(m_resItr->get(), 0, ifield));
}

//------------------------------------------------------------------------------
// columnOptionalUint16
//------------------------------------------------------------------------------
std::optional<uint16_t> PostgresRset::columnOptionalUint16(const std::string& colName) const {
  const int ifield = getColumnIndex(colName);

  // the value can be null
  if (PQgetisnull(m_resItr->get(), 0, ifield)) {
    return std::nullopt;
  }

  const std::string stringValue(PQgetvalue(m_resItr->get(), 0, ifield));

  if (!utils::isValidUInt(stringValue)) {
    throw exception::Exception(std::string("Column ") + colName + " contains the value " + stringValue +
                               " which is not a valid unsigned integer");
  }

  return utils::toUint16(stringValue);
}

//------------------------------------------------------------------------------
// columnOptionalUint32
//------------------------------------------------------------------------------
std::optional<uint32_t> PostgresRset::columnOptionalUint32(const std::string& colName) const {
  const int ifield = getColumnIndex(colName);

  // the value can be null
  if (PQgetisnull(m_resItr->get(), 0, ifield)) {
    return std::nullopt;
  }

  const std::string stringValue(PQgetvalue(m_resItr->get(), 0, ifield));

  if (!utils::isValidUInt(stringValue)) {
    throw exception::Exception(std::string("Column ") + colName + " contains the value " + stringValue +
                               " which is not a valid unsigned integer");
  }

  return utils::toUint32(stringValue);
}

//------------------------------------------------------------------------------
// columnOptionalUint64
//------------------------------------------------------------------------------
std::optional<uint64_t> PostgresRset::columnOptionalUint64(const std::string& colName) const {
  const int ifield = getColumnIndex(colName);

  // the value can be null
  if (PQgetisnull(m_resItr->get(), 0, ifield)) {
    return std::nullopt;
  }

  const std::string stringValue(PQgetvalue(m_resItr->get(), 0, ifield));

  if (!utils::isValidUInt(stringValue)) {
    throw exception::Exception(std::string("Column ") + colName + " contains the value " + stringValue +
                               " which is not a valid unsigned integer");
  }

  return utils::toUint64(stringValue);
}

//------------------------------------------------------------------------------
// columnOptionalDouble
//------------------------------------------------------------------------------
std::optional<double> PostgresRset::columnOptionalDouble(const std::string& colName) const {
  const int ifield = getColumnIndex(colName);

  // the value can be null
  if (PQgetisnull(m_resItr->get(), 0, ifield)) {
    return std::nullopt;
  }

  const std::string stringValue(PQgetvalue(m_resItr->get(), 0, ifield));

  if (!utils::isValidDecimal(stringValue)) {
    throw exception::Exception(std::string("Column ") + colName + " contains the value " + stringValue +
                               " which is not a valid decimal");
  }

  return utils::toDouble(stringValue);
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string& PostgresRset::getSql() const {
  return m_stmt.getSql();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool PostgresRset::next() {
  // always locks in order statement and then connection
  threading::RWLockWrLocker locker2(m_stmt.m_lock);
  threading::RWLockWrLocker locker(m_conn.m_lock);
  // trying to reuse the indices per the full result set
  // m_columnPQindexCache.clear();

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

}  // namespace cta::rdbms::wrapper
