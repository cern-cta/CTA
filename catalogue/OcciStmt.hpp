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

#include "catalogue/ColumnNameToIdx.hpp"

#include <mutex>
#include <occi.h>
#include <stdint.h>

namespace cta {
namespace catalogue {

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
 *
 * Please note that this wrapper does not expose any data types that are
 * different with respect to _GLIBCXX_USE_CXX11_ABI.  For example this wrapper
 * does not expose the std::string data type.
 */
class OcciStmt {
public:

  /**
   * Constructor.
   *
   * This constructor will throw an exception if either the sql or stmt
   * parameters are NULL.
   *
   * @param sql The SQL statement.
   * @param conn The database connection.
   * @param stmt The prepared statement.
   */
  OcciStmt(const char *const sql, OcciConn &conn,
    oracle::occi::Statement *const stmt);

  /**
   * Destructor.
   */
  ~OcciStmt() throw();

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  void close();

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  const char *getSql() const;

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
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bind(const char *const paramName, const uint64_t paramValue);

  /** 
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bind(const char*paramName, const char *paramValue);

  /**
   *  Executes the statement and returns the result set.
   */
  OcciRset *executeQuery();

private:

  /**
   * Mutex used to serialize access to this object.
   */
  std::mutex m_mutex;

  /**
   * The SQL statement.
   */
  char *m_sql;

  /**
   * The database connection.
   */
  OcciConn &m_conn;

  /**
   * The prepared statement.
   */
  oracle::occi::Statement *m_stmt;

}; // class OcciStmt

} // namespace catalogue
} // namespace cta
