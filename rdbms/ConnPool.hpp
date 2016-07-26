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

#include "Conn.hpp"
#include "ConnFactory.hpp"
#include "Login.hpp"
#include "PooledConn.hpp"

#include <condition_variable>
#include <memory>
#include <mutex>

namespace cta {
namespace rdbms {

/**
 * A pool of database connections.
 */
class ConnPool {
public:

  /**
   * Constructor.
   *
   * @param connFactory Factory to be used to create new database connections.
   * Please note that the ConnPool will take ownership of the factory and
   * therefore the destructor of ConnPool will delete the factory.
   * @param nbConns The number of database connections within the pool.
   */
  ConnPool(std::unique_ptr<ConnFactory> connFactory, const uint64_t nbConns);

  /**
   * Destructor.
   */
  ~ConnPool() throw();

  /**
   * Takes a connection from the pool.
   *
   * Please note that this method will block if the pool is empty.  In such a
   * situation this method will unblock when a connection is returned to the
   * pool.
   *
   * @return A connection from the pool.
   */
  PooledConn getConn();

  /**
   * Returns the specified database connection to the pool.
   *
   * @param conn The connection to be returned to the pool.
   */
  void returnConn(Conn *const conn);

private:

  /**
   * The database connection factory for the pool.
   */
  std::unique_ptr<ConnFactory> m_connFactory;

  /**
   * The number of database connections within the pool.
   */
  uint64_t m_nbConns;

  /**
   * Mutex used to serialize access to the database connections within the pool.
   */
  std::mutex m_connsMutex;

  /**
   * Condition variable used by threads returning connections to the pool to
   * notify threads waiting for connections.
   */
  std::condition_variable m_connsCv;

  /**
   * The database connections within the pool.
   */
  std::list<Conn *> m_conns;

  /**
   * Creates the database connection factory for the pool based on the specified
   * login details.
   *
   * @param login The database login details to be used to create new
   * connections.
   */
  void createConnFactory(const Login &login);

  /**
   * Creates the specified number of database connections with the pool.
   *
   * @param nbConns The number of database connections to be created.
   */
  void createConns(const uint64_t nbConns);

}; // class ConnPool

} // namespace rdbms
} // namespace cta
