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

#pragma once

#include "rdbms/AutocommitMode.hpp"
#include "rdbms/Constants.hpp"
#include "rdbms/wrapper/ParamNameToIdx.hpp"
#include "rdbms/wrapper/RsetWrapper.hpp"

#include <memory>
#include <optional>
#include <stdint.h>
#include <string>

namespace cta::rdbms::wrapper {

/**
 * Abstract class specifying the interface to a database statement.
 */
class StmtWrapper {
public:

  /**
   * Constructor
   *
   * @param sql The SQL statement
   */
  explicit StmtWrapper(const std::string& sql);

  /**
   * Destructor.
   */
  virtual ~StmtWrapper() = 0;

  /**
   * Deletion of the copy constructor.
   */
  StmtWrapper(StmtWrapper &) = delete;

  /**
   * Deletion of the move constructor.
   */
  StmtWrapper(StmtWrapper &&) = delete;

  /**
   * Deletion of the copy assignment operator.
   */
  StmtWrapper &operator=(const StmtWrapper &) = delete;

  /**
   * Deletion of the move assignment operator.
   */
  StmtWrapper &operator=(StmtWrapper &&) = delete;

  /**
   * Clears the prepared statement so that it is ready to be reused.
   */
  virtual void clear() = 0;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close() = 0;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  const std::string &getSql() const;

  /**
   * Returns the index of the specified SQL parameter.
   *
   * @param paramName The name of the SQL parameter.
   * @return The index of the SQL parameter.
   */
  uint32_t getParamIdx(const std::string &paramName) const;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindUint8(const std::string &paramName, const std::optional<uint8_t> &paramValue) = 0;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindUint16(const std::string &paramName, const std::optional<uint16_t> &paramValue) = 0;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindUint32(const std::string &paramName, const std::optional<uint32_t> &paramValue) = 0;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindUint64(const std::string &paramName, const std::optional<uint64_t> &paramValue) = 0;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindDouble(const std::string &paramName, const std::optional<double> &paramValue) = 0;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindBool(const std::string &paramName, const std::optional<bool> &paramValue);

  /**
   * Binds an SQL parameter of type binary string (byte array).
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindBlob(const std::string &paramName, const std::string &paramValue) = 0;

  /**
   * Binds an SQL parameter of type optional-string.
   *
   * Please note that this method will throw an exception if the optional string
   * parameter has the empty string as its value.  An optional string parameter
   * should either have a non-empty string value or no value at all.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindString(const std::string &paramName, const std::string &paramValue) {
    const std::optional<std::string> optionalParamValue(paramValue);
    bindString(paramName, optionalParamValue);
  }

  /**
   * Binds an SQL parameter of type optional-string.
   *
   * Please note that this method will throw an exception if the optional string
   * parameter has the empty string as its value.  An optional string parameter
   * should either have a non-empty string value or no value at all.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindString(const std::string &paramName, const std::optional<std::string> &paramValue) = 0;

  /**
   *  Executes the statement and returns the result set.
   *
   *  @return The result set.
   */
  virtual std::unique_ptr<RsetWrapper> executeQuery() = 0;

  /**
   * Executes the statement.
   */
  virtual void executeNonQuery() = 0;

  /**
   * Returns the number of rows affected by the last execution of this
   * statement.
   *
   * @return The number of affected rows.
   */
  virtual uint64_t getNbAffectedRows() const = 0;

  /**
   * Returns the SQL string to be used in an exception message.  The string
   * will be clipped at a maxmum of c_maxSqlLenInExceptions characters.  If the
   * string is actually clipped then the three last characters will be an
   * replaced by an ellipsis of three dots, in other word "...".  These 3
   * characters will indicate to the reader of the exception message that the
   * SQL statement has been clipped.
   *
   * @param maxSqlLenInExceptions The maximum length an SQL statement can have
   * in an exception error message.
   * @return The SQL string to be used in an exception message.
   */
  std::string getSqlForException(const std::string::size_type maxSqlLenInExceptions = MAX_SQL_LEN_IN_EXCEPTIONS) const;

private:

  /**
   * The SQL statement.
   */
  std::string m_sql;

  /**
   * Map from SQL parameter name to parameter index.
   */
  ParamNameToIdx m_paramNameToIdx;

}; // class StmtWrapper

} // namespace cta::rdbms::wrapper
