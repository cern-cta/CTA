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

#include "DbStmt.hpp"
#include "ParamNameToIdx.hpp"

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
class SqliteStmt: public DbStmt {
public:

  /**
   * Constructor.
   *
   * @param conn The database connection.
   * @param sql The SQL statement.
   * @param stmt The prepared statement.
   */
  SqliteStmt(SqliteConn &conn, const std::string &sql, sqlite3_stmt *const stmt);

  /**
   * Destructor.
   */
  virtual ~SqliteStmt() throw();

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close();

  /**
   * Returns a pointer to the underlying prepared statement.
   *
   * This method throws an exception if the pointer to the underlying prepared
   * statement is NULL.
   *
   * @return the underlying prepared statement.
   */
  sqlite3_stmt *get() const;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  virtual const std::string &getSql() const;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindUint64(const std::string &paramName, const uint64_t paramValue);

  /** 
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  virtual void bindString(const std::string &paramName, const std::string &paramValue);

  /**
   * Executes the statement and returns the result set.
   *
   * @return The result set.  Please note that it is the responsibility of the
   * caller to free the memory associated with the result set.
   */
  virtual DbRset *executeQuery();

  /**
   * Executes the statement.
   */
  virtual void executeNonQuery();

  /**
   * Returns the number of rows affected by the last execution of this
   * statement.
   *
   * @return The number of affected rows.
   */
  virtual uint64_t getNbAffectedRows() const;

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
   * The SQL statement.
   */
  std::string m_sql;

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

}; // class SqlLiteStmt

} // namespace rdbms
} // namespace cta
