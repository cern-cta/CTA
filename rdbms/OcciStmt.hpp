/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#pragma once

#include "rdbms/Stmt.hpp"

#include <memory>
#include <mutex>
#include <occi.h>
#include <stdint.h>

namespace cta {
namespace rdbms {

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

/**
 * A convenience wrapper around an OCCI prepared statement.
 */
class OcciStmt: public Stmt {
public:

  /**
   * Constructor.
   *
   * @param autocommitMode The autocommit mode of the statement.
   * @param sql The SQL statement.
   * @param conn The database connection.
   * @param stmt The OCCI statement.
   */
  OcciStmt(
    const AutocommitMode autocommitMode,
    const std::string &sql,
    OcciConn &conn,
    oracle::occi::Statement *const stmt);

  /**
   * Destructor.
   */
  ~OcciStmt() throw() override;

  /**
   * Prevent copying the object.
   */
  OcciStmt(const OcciStmt &) = delete;

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
  void bindUint64(const std::string &paramName, const uint64_t paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindOptionalUint64(const std::string &paramName, const optional<uint64_t> &paramValue) override;

  /** 
   * Binds an SQL parameter of type string.
   *
   * Please note that this method will throw an exception if the string
   * parameter is empty.  If a null value is to be bound then the
   * bindOptionalString() method should be used.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bindString(const std::string &paramName, const std::string &paramValue) override;

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
  void bindOptionalString(const std::string &paramName, const optional<std::string> &paramValue) override;

  /**
   *  Executes the statement and returns the result set.
   *
   *  @return The result set.  Please note that it is the responsibility of the
   *  caller to free the memory associated with the result set.
   */
  std::unique_ptr<Rset> executeQuery() override;

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

private:

  /**
   * Mutex used to serialize access to this object.
   */
  std::mutex m_mutex;

  /**
   * The database connection.
   */
  OcciConn &m_conn;

  /**
   * The prepared statement.
   */
  oracle::occi::Statement *m_stmt;

  /**
   * Determines whether or not the connection should be closed based on the
   * specified Oracle exception.
   *
   * @param ex The Oracle exception.
   * @return True if the connection should be closed.
   */
  static bool connShouldBeClosed(const oracle::occi::SQLException &ex);

}; // class OcciStmt

} // namespace rdbms
} // namespace cta
