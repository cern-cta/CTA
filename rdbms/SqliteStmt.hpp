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

#include "rdbms/ParamNameToIdx.hpp"
#include "rdbms/Stmt.hpp"

#include <map>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <sqlite3.h>

namespace cta {
namespace rdbms {

class SqliteConn;
class SqliteRset;

/**
 * A convenience wrapper around an SQLite prepared statement.
 */
class SqliteStmt: public Stmt {
public:

  /**
   * Constructor.
   *
   * This method is called by the SqliteStmt::createStmt() method and assumes
   * that a lock has already been taken on SqliteStmt::m_mutex;
   *
   * @param autocommitMode The autocommit mode of the statement.
   * @param conn The database connection.
   * @param sql The SQL statement.
   */
  SqliteStmt(const AutocommitMode autocommitMode, SqliteConn &conn, const std::string &sql);

  /**
   * Destructor.
   */
  virtual ~SqliteStmt() throw() override;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close() override;

  /**
   * Returns a pointer to the underlying prepared statement.
   *
   * This method throws an exception if the pointer to the underlying prepared
   * statement is nullptr.
   *
   * @return the underlying prepared statement.
   */
  sqlite3_stmt *get() const;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindUint64(const std::string &paramName, const uint64_t paramValue) override;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindOptionalUint64(const std::string &paramName, const optional<uint64_t> &paramValue) override;

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
  virtual void bindString(const std::string &paramName, const std::string &paramValue) override;

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
  virtual void bindOptionalString(const std::string &paramName, const optional<std::string> &paramValue) override;

  /**
   * Executes the statement and returns the result set.
   *
   * @return The result set.  Please note that it is the responsibility of the
   * caller to free the memory associated with the result set.
   */
  virtual std::unique_ptr<Rset> executeQuery() override;

  /**
   * Executes the statement.
   */
  virtual void executeNonQuery() override;

  /**
   * Returns the number of rows affected by the last execution of this
   * statement.
   *
   * @return The number of affected rows.
   */
  virtual uint64_t getNbAffectedRows() const override;

private:

  /**
   * Mutex used to serialize access to the prepared statement.
   */
  std::mutex m_mutex;

  /**
   * The SQL connection.
   */
  SqliteConn &m_conn;

  /**
   * Map from SQL parameter name to parameter index.
   */
  ParamNameToIdx m_paramNameToIdx;

  /**
   * The prepared statement.
   */
  sqlite3_stmt *m_stmt;

  /**
   * The number of rows affected by the last execution of this statement.
   */
  uint64_t m_nbAffectedRows;

  /**
   * Begins an SQLite deferred transaction.
   *
   * This method is called by the constructor which in turn was called by the
   * SqliteStmt::createStmt() method and assumes that a lock has already been
   * taken on SqliteStmt::m_mutex;
   */
  void beginDeferredTransaction();

}; // class SqlLiteStmt

} // namespace rdbms
} // namespace cta
