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

#include "catalogue/DbStmt.hpp"

#include <map>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <sqlite3.h>

namespace cta {
namespace catalogue {

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
   * @param sql The SQL statement.
   * @param stmt The prepared statement.
   */
  SqliteStmt(const char *const sql, sqlite3_stmt *const stmt);

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
  virtual const char *getSql() const;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindUint64(const char *const paramName, const uint64_t paramValue);

  /** 
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  virtual void bind(const char *const paramName, const char *const paramValue);

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
  void executeNonQuery();

private:

  /**
   * Mutex used to serialize access to the prepared statement.
   */
  std::mutex m_mutex;

  /**
   * The SQL statement.
   *
   * A C string is used instead of an std::string so that this class can be used
   * by code compiled against the CXX11 ABI and code compiled against a
   * pre-CXX11 ABI.
   */
  std::unique_ptr<char[]> m_sql;

  /**
   * The prepared statement.
   */
  sqlite3_stmt *m_stmt;

  /**
   * Returns the index of the SQL parameter with the specified name.
   *
   * This method throws an exception if the parameter is not found.
   *
   * @param paramName The name of the SQL parameter.
   * @return The index of the SQL parameter.
   */
  int getParamIndex(const char *const paramName) const;

}; // class SqlLiteStmt

} // namespace catalogue
} // namespace cta
