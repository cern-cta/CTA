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

#include "rdbms/DbConn.hpp"

namespace cta {
namespace rdbms {

/**
 * Forward declaration to avoid circular dependency between PooledDbCon and
 * DbConnPool.
 */
class DbConnPool;

/**
 * A smart database connection that will automatically return itself to its
 * parent connection pool when it goes out of scope.
 */
class PooledDbConn {
public:

  /**
   * Constructor.
   *
   * @param dbConn The database connection.
   * @param dbConnPool The database connection pool to which the connection
   * should be returned.
   */
  PooledDbConn(DbConn *const dbConn, DbConnPool *const dbConnPool) noexcept;

  /**
   * Deletion of the copy constructor.
   */
  PooledDbConn(PooledDbConn &) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object.
   */
  PooledDbConn(PooledDbConn &&other) noexcept;

  /**
   * Destructor.
   *
   * Returns the database connection back to its pool.
   */
  ~PooledDbConn() noexcept;

  /**
   * Returns the owned database connection.
   *
   * @return The owned database connection.
   */
  DbConn *get() const noexcept;
  /**
   * Returns the owned database connection.
   *
   * @return The owned database connection.
   */
  DbConn *operator->() const noexcept;

  /**
   * Returns the owned database connection.
   *
   * This method throws an exception if this smart database connection does not
   * currently own a database connection.
   *
   * @return The owned database connection.
   */
  DbConn &operator*() const;

  /**
   * Deletion of the copy assignment operator.
   */
  PooledDbConn &operator=(const PooledDbConn &) = delete;

  /**
   * Move assignment operator.
   *
   * @param rhs The object on the right-hand side of the operator.
   * @return This object.
   */
  PooledDbConn &operator=(PooledDbConn &&rhs);

  /**
   * Structure to store a database connection and the pool to which it belongs.
   */
  struct DbConnAndPool {
    DbConn *conn;
    DbConnPool *pool;

    DbConnAndPool() noexcept: conn(NULL), pool(NULL) {
    }

    DbConnAndPool(DbConn *conn, DbConnPool *pool) noexcept:
      conn(conn),
      pool(pool) {
    }
  };

  /**
   * Takes ownership of the specified database connection.  If this smart
   * connection already owns a connection that is not the same as the one
   * specified then the already owned connection will be returned to its pool.
   *
   * @param dbConnAndPool The database connection to be owned and the pool to
   * which it belongs.
   */
  void reset(const DbConnAndPool &dbConnAndPool = DbConnAndPool());

  /**
   * Releases the owned database connection.
   *
   * @return The released database connection and the pool to which it belongs.
   */
  DbConnAndPool release() noexcept;

private:

  /**
   * The database connection owned by this smart connection and the pool to
   * which the connection belongs.
   */
  DbConnAndPool m_dbConnAndPool;

}; // class PooledDbConn

} // namespace rdbms
} // namespace cta
