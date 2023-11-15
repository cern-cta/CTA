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

#include "common/threading/Mutex.hpp"
#include "rdbms/wrapper/StmtWrapper.hpp"

#include <memory>
#include <occi.h>
#include <stdint.h>

namespace cta::rdbms::wrapper {

/**
 * Forward declaration to avoid a circular dependency between OcciStmt and
 * OcciConn.
 */
class OcciConn;

/**
 * Forward declaration to avoid a circular dependency between OcciStmt and
 * OcciRset.
 */
class OcciRset;

class OcciColumn;

/**
 * A convenience wrapper around an OCCI prepared statement.
 */
class OcciStmt: public StmtWrapper {
public:

  /**
   * Constructor.
   *
   * @param sql The SQL statement.
   * @param conn The database connection.
   * @param stmt The OCCI statement.
   */
  OcciStmt(
    const std::string &sql,
    OcciConn &conn,
    oracle::occi::Statement *const stmt);

  /**
   * Destructor.
   */
  ~OcciStmt() override;

  /**
   * Prevent copying the object.
   */
  OcciStmt(const OcciStmt &) = delete;

  /**
   * Clears the prepared statement so that it is ready to be reused.
   */
  void clear() override;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  void close() override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint8(const std::string &paramName, const std::optional<uint8_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint16(const std::string &paramName, const std::optional<uint16_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint32(const std::string &paramName, const std::optional<uint32_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint64(const std::string &paramName, const std::optional<uint64_t> &paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindDouble(const std::string &paramName, const std::optional<double> &paramValue) override;

  /** 
   * Binds an SQL parameter of type binary string (byte array).
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bindBlob(const std::string &paramName, const std::string &paramValue) override;

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
  void bindString(const std::string &paramName, const std::optional<std::string> &paramValue) override;

  /**
   * Executes the statement and returns the result set.
   *
   * @return The result set.
   */
  std::unique_ptr<RsetWrapper> executeQuery() override;

  /**
   * Executes the statement.
   */
  void executeNonQuery() override;

  /**
   * Returns the number of rows affected by the last execution of this
   * statement.
   *
   * @return The number of affected rows.
   */
  uint64_t getNbAffectedRows() const override;

  /**
   * Returns the underlying OCCI result set.
   *
   * @return The underlying OCCI result set.
   */
  oracle::occi::Statement *get() const;

  /**
   * Alias for the get() method.
   *
   * @return The underlying OCCI result set.
   */
  oracle::occi::Statement *operator->() const;

  /**
   * Sets the specified column data for a batch-based database access.
   *
   * @param  col   The column data
   * @param  type  The type of the data
   */
  void setColumn(OcciColumn &col, oracle::occi::Type type = oracle::occi::OCCI_SQLT_STR);

  /**
   * Determines whether or not the connection should be closed based on the
   * specified Oracle exception.
   *
   * @param ex The Oracle exception.
   * @return True if the connection should be closed.
   */
  static bool connShouldBeClosed(const oracle::occi::SQLException &ex);

private:

  /**
   * Mutex used to serialize access to this object.
   */
  threading::Mutex m_mutex;

  /**
   * The database connection.
   */
  OcciConn &m_conn;

  /**
   * The prepared statement.
   */
  oracle::occi::Statement *m_stmt;

  /**
   * Templated bind of an optional number.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  template <typename IntegerType> void bindInteger(const std::string &paramName,
    const std::optional<IntegerType> &paramValue) {
    try {
      const unsigned paramIdx = getParamIdx(paramName);
      if (paramValue) {
        // Bind integer as a string in order to support 64-bit integers
        m_stmt->setString(paramIdx, std::to_string(paramValue.value()));
      } else {
        m_stmt->setNull(paramIdx, oracle::occi::OCCINUMBER);
      }
    } catch (exception::Exception &ex) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
                                 getSqlForException() + ": " + ex.getMessage().str());
    } catch (std::exception &se) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
                                 getSqlForException() + ": " + se.what());
    }
  }
}; // class OcciStmt

} // namespace cta::rdbms::wrapper
