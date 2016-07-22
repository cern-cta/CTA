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
class PooledConn {
public:

  /**
   * Constructor.
   *
   * @param conn The database connection.
   * @param connPool The database connection pool to which the connection
   * should be returned.
   */
  PooledConn(Conn *const conn, ConnPool *const connPool) noexcept;

  /**
   * Deletion of the copy constructor.
   */
  PooledConn(PooledConn &) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object.
   */
  PooledConn(PooledConn &&other) noexcept;

  /**
   * Destructor.
   *
   * Returns the database connection back to its pool.
   */
  ~PooledConn() noexcept;

  /**
   * Returns the owned database connection.
   *
   * @return The owned database connection.
   */
  Conn *get() const noexcept;
  /**
   * Returns the owned database connection.
   *
   * @return The owned database connection.
   */
  Conn *operator->() const noexcept;

  /**
   * Returns the owned database connection.
   *
   * This method throws an exception if this smart database connection does not
   * currently own a database connection.
   *
   * @return The owned database connection.
   */
  Conn &operator*() const;

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
   * Structure to store a database connection and the pool to which it belongs.
   */
  struct ConnAndPool {
    Conn *conn;
    ConnPool *pool;

    ConnAndPool() noexcept: conn(nullptr), pool(nullptr) {
    }

    ConnAndPool(Conn *c, ConnPool *p) noexcept:
      conn(c),
      pool(p) {
    }
  };

  /**
   * Takes ownership of the specified database connection.  If this smart
   * connection already owns a connection that is not the same as the one
   * specified then the already owned connection will be returned to its pool.
   *
   * @param connAndPool The database connection to be owned and the pool to
   * which it belongs.
   */
  void reset(const ConnAndPool &connAndPool = ConnAndPool());

  /**
   * Releases the owned database connection.
   *
   * @return The released database connection and the pool to which it belongs.
   */
  ConnAndPool release() noexcept;

private:

  /**
   * The database connection owned by this smart connection and the pool to
   * which the connection belongs.
   */
  ConnAndPool m_connAndPool;

}; // class PooledConn

} // namespace rdbms
} // namespace cta
