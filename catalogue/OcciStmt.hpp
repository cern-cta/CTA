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
#include <string>

namespace cta {
namespace catalogue {

/**
 * Forward decalaration to avoid a circular depedency between OcciConn and
 * OcciStmt.
 */
class OcciConn;

/**
 * A convenience wrapper around an OCCI prepared statement.
 */
class OcciStmt {
public:

  /**
   * Constructor.
   *  
   * @param sql The SQL statement.
   * @param conn The database connection.
   * @param stmt The prepared statement.
   */
  OcciStmt(const std::string &sql, OcciConn &conn,
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
  const std::string &getSql() const;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bind(const std::string &paramName, const uint64_t paramValue);

  /** 
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bind(const std::string &paramName, const std::string &paramValue);

private:

  /**
   * Mutex used to serialize access to the prepared statement.
   */
  std::mutex m_mutex;

  /**
   * The SQL statement.
   */
  const std::string m_sql;

  /**
   * The database connection.
   */
  OcciConn &m_conn;

  /**
   * The prepared statement.
   */
  oracle::occi::Statement *m_stmt;

}; // class SqlLiteStmt

} // namespace catalogue
} // namespace cta
