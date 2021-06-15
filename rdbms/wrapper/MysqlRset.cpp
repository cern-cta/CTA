/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "common/exception/Exception.hpp"
#include "common/exception/Errnum.hpp"
#include "rdbms/ConstraintError.hpp"
#include "rdbms/NullDbValue.hpp"
#include "rdbms/PrimaryKeyError.hpp"
#include "rdbms/wrapper/Mysql.hpp"
#include "rdbms/wrapper/MysqlRset.hpp"
#include "rdbms/wrapper/MysqlStmt.hpp"

#include <iostream>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>

#include <mysql.h>

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MysqlRset::MysqlRset(MysqlStmt &stmt, Mysql::FieldsInfo &fields)
  : m_stmt(stmt), m_fields(fields) {

}

//------------------------------------------------------------------------------
// destructor.
//------------------------------------------------------------------------------
MysqlRset::~MysqlRset() {

}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &MysqlRset::getSql() const {
  return m_stmt.getSql();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool MysqlRset::next() {

  int rc = mysql_stmt_fetch(m_stmt.get());
  if (rc == MYSQL_NO_DATA) {
    return false;
  } else if (rc == MYSQL_DATA_TRUNCATED) {
    throw exception::Exception(std::string(__FUNCTION__) + " data truncated.");
  } else if (rc == 1) {
    std::string msg = mysql_stmt_error(m_stmt.get());
    throw exception::Exception(std::string(__FUNCTION__) + " " + msg);
  } else if (rc == 0) {
    // it is ok
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " unknown rc: " + std::to_string(rc));
  }

  return true;
}

//------------------------------------------------------------------------------
// columnIsNull
//------------------------------------------------------------------------------
bool MysqlRset::columnIsNull(const std::string &colName) const {
  if (not m_fields.exists(colName)) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
    return false;
  }
  
  Mysql::Placeholder* holder = m_stmt.columnHolder(colName);

  return *holder->get_is_null();
}

std::string MysqlRset::columnBlob(const std::string &colName) const {
  auto blob = columnOptionalString(colName);
  return blob ? *blob : std::string();
}

//------------------------------------------------------------------------------
// columnOptionalString
//------------------------------------------------------------------------------
optional<std::string> MysqlRset::columnOptionalString(const std::string &colName) const {
  if (not m_fields.exists(colName)) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
    return nullopt;
  }

  Mysql::Placeholder* holder = m_stmt.columnHolder(colName); // m_holders[idx];

  // the value can be null
  if (holder->get_is_null() and *holder->get_is_null()) {
    return nullopt;
  }

  return optional<std::string>(holder->get_string());
}

//------------------------------------------------------------------------------
// columnOptionalUint8
//------------------------------------------------------------------------------
optional<uint8_t> MysqlRset::columnOptionalUint8(const std::string &colName) const {
  if (not m_fields.exists(colName)) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
    return nullopt;
  }

  Mysql::Placeholder* holder = m_stmt.columnHolder(colName); // m_holders[idx];

  // the value can be null
  if (holder->get_is_null() and *holder->get_is_null()) {
    return nullopt;
  }

  return optional<uint8_t>(holder->get_uint8());
}

//------------------------------------------------------------------------------
// columnOptionalUint16
//------------------------------------------------------------------------------
optional<uint16_t> MysqlRset::columnOptionalUint16(const std::string &colName) const {
  if (not m_fields.exists(colName)) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
    return nullopt;
  }

  Mysql::Placeholder* holder = m_stmt.columnHolder(colName); // m_holders[idx];

  // the value can be null
  if (holder->get_is_null() and *holder->get_is_null()) {
    return nullopt;
  }

  return optional<uint16_t>(holder->get_uint16());
}

//------------------------------------------------------------------------------
// columnOptionalUint32
//------------------------------------------------------------------------------
optional<uint32_t> MysqlRset::columnOptionalUint32(const std::string &colName) const {
  if (not m_fields.exists(colName)) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
    return nullopt;
  }

  Mysql::Placeholder* holder = m_stmt.columnHolder(colName); // m_holders[idx];

  // the value can be null
  if (holder->get_is_null() and *holder->get_is_null()) {
    return nullopt;
  }

  return optional<uint32_t>(holder->get_uint32());
}

//------------------------------------------------------------------------------
// columnOptionalUint64
//------------------------------------------------------------------------------
optional<uint64_t> MysqlRset::columnOptionalUint64(const std::string &colName) const {
  if (not m_fields.exists(colName)) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
    return nullopt;
  }

  Mysql::Placeholder* holder = m_stmt.columnHolder(colName); // m_holders[idx];

  // the value can be null
  if (holder->get_is_null() and *holder->get_is_null()) {
    return nullopt;
  }

  return optional<uint64_t>(holder->get_uint64());
}

//------------------------------------------------------------------------------
// columnOptionalDouble
//------------------------------------------------------------------------------
optional<double> MysqlRset::columnOptionalDouble(const std::string &colName) const {
  if (not m_fields.exists(colName)) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
    return nullopt;
  }

  Mysql::Placeholder* holder = m_stmt.columnHolder(colName); // m_holders[idx];

  // the value can be null
  if (holder->get_is_null() and *holder->get_is_null()) {
    return nullopt;
  }

  return optional<double>(holder->get_double());
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
