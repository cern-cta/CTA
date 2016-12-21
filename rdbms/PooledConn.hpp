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

#include <list>
#include <memory>

namespace cta {
namespace rdbms {

class Conn;

class ConnPool;

/**
 * A smart database connection that will automatically return the underlying
 * database connection to its parent connection pool when it goes out of scope.
 */
class PooledConn {
public:

  /**
   * Constructor.
   *
   * @param conn The database connection.
   * @param pool The database connection pool to which the connection
   * should be returned.
   */
  PooledConn(std::unique_ptr<Conn> conn, ConnPool *const pool);

  /**
   * Deletion of the copy constructor.
   */
  PooledConn(PooledConn &) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object.
   */
  PooledConn(PooledConn &&other);

  /**
   * Destructor.
   *
   * Returns the database connection back to its pool.
   */
  ~PooledConn() noexcept;

  /**
   * Deletion of the copy assignment operator.
   */
  PooledConn &operator=(const PooledConn &) = delete;

  /**
   * Move assignment operator.
   *
   * @param rhs The object on the right-hand side of the operator.
   * @return This object.
   */
  PooledConn &operator=(PooledConn &&rhs);

  /**
   * Creates a prepared statement.
   *
   * @param sql The SQL statement.
   * @param autocommitMode The autocommit mode of the statement.
   * @return The prepared statement.
   */
  std::unique_ptr<Stmt> createStmt(const std::string &sql, const Stmt::AutocommitMode autocommitMode);

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
  void commit();

  /**
   * Rolls back the current transaction.
   */
  void rollback();

  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  std::list<std::string> getTableNames();

  /**
   * Returns true if this connection is open.
   */
  bool isOpen() const;

private:

  /**
   * The database connection.
   */
  std::unique_ptr<Conn> m_conn;

  /**
   * The database connection pool to which the m_conn should be returned.
   */
  ConnPool *m_pool;

}; // class PooledConn

} // namespace rdbms
} // namespace cta
