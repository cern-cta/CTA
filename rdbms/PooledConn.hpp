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

#include "rdbms/Conn.hpp"

#include <memory>

namespace cta {
namespace rdbms {

/**
 * Forward declaration to avoid circular dependency between PooledDbCon and
 * ConnPool.
 */
class ConnPool;

/**
 * A smart database connection that will automatically return itself to its
 * parent connection pool when it goes out of scope.
 */
class PooledConn: public Conn {
public:

  /**
   * Constructor.
   *
   * @param conn The database connection.
   * @param pool The database connection pool to which the connection
   * should be returned.
   */
  PooledConn(Conn *const conn, ConnPool *const pool);

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
   * Idempotent close() method.  The destructor calls this method.
   */
  void close() override;

  /**
   * Creates a prepared statement.
   *
   * @param sql The SQL statement.
   * @param autocommitMode The autocommit mode of the statement.
   * @return The prepared statement.
   */
  std::unique_ptr<Stmt> createStmt(const std::string &sql, const Stmt::AutocommitMode autocommitMode) override;

  /**
   * Commits the current transaction.
   */
  void commit() override;

  /**
   * Rolls back the current transaction.
   */
  void rollback() override;

  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  std::list<std::string> getTableNames() override;

  /**
   * Returns true if this connection is open.
   */
  bool isOpen() const override;

private:

  /**
   * The database connection.
   */
  Conn *m_conn = nullptr;

  /**
   * The database connection pool to which the m_conn should be returned.
   */
  ConnPool *m_pool = nullptr;

}; // class PooledConn

} // namespace rdbms
} // namespace cta
