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

#include "Stmt.hpp"

#include <atomic>
#include <list>
#include <memory>
#include <string>

namespace cta {
namespace rdbms {

/**
 * Abstract class that specifies the interface to a database connection.
 */
class Conn {
public:

  /**
   * Destructor.
   */
  virtual ~Conn() throw() = 0;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close() = 0;

  /**
   * Creates a prepared statement.
   *
   * @param sql The SQL statement.
   * @param autocommitMode The autocommit mode of the statement.
   * @return The prepared statement.
   */
  virtual std::unique_ptr<Stmt> createStmt(const std::string &sql, const Stmt::AutocommitMode autocommitMode) = 0;

  /**
   * Convenience method that parses the specified string of multiple SQL
   * statements and calls executeNonQuery() for each individual statement found.
   *
   * Please note that each statement should be a non-query terminated by a
   * semicolon and that each individual statement will be executed with
   * autocommit ON.
   *
   * @param sqlStmts The SQL statements to be executed.
   * @param autocommitMode The autocommit mode of the statement.
   */
  void executeNonQueries(const std::string &sqlStmts);

  /**
   * Convenience method that wraps Conn::createStmt() followed by
   * Stmt::executeNonQuery().
   *
   * @param sql The SQL statement.
   * @param autocommitMode The autocommit mode of the statement.
   */
  void executeNonQuery(const std::string &sql, const Stmt::AutocommitMode autocommitMode);

  /**
   * Commits the current transaction.
   */
  virtual void commit() = 0;

  /**
   * Rolls back the current transaction.
   */
  virtual void rollback() = 0;

  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  virtual std::list<std::string> getTableNames() = 0;

  /**
   * Returns true if the connection is healthy.
   *
   * @return True if the connection is healthy.
   */
  bool getHealthy() const;

protected:

  /**
   * Sets the status of the connection to be either healthy or not healthy.
   *
   * @param value True if the connection is healthy.
   */
  void setHealthy(const bool value);

private:

  /**
   * True if the connection's state is healthy.
   */
  std::atomic<bool> m_healthy{true};

}; // class Conn

} // namespace rdbms
} // namespace cta
