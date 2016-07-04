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

#include "DbConn.hpp"
#include "DbConnFactory.hpp"
#include "DbLogin.hpp"

#include <condition_variable>
#include <memory>
#include <mutex>

namespace cta {
namespace rdbms {

/**
 * A pool of database connections.
 */
class DbConnPool {
public:

  /**
   * Constructor.
   *
   * @param dbLogin The database login details to be used to create new
   * connections.
   * @param nbDbConns The number of database connections within the pool.
   */
  DbConnPool(const DbLogin &dbLogin, const uint64_t nbDbConns);

  /**
   * Destructor.
   */
  ~DbConnPool() throw();

  /**
   * Takes a connection from the pool.
   *
   * Please note that this method will block if the pool is empty.  In such a
   * situation this method will unblock when a connection is returned to the
   * pool.
   *
   * @return A connection from the pool.
   */
  DbConn *getDbConn();

  /**
   * Returns the specified database connection to the pool.
   *
   * @param dbConn The connection to be returned to the pool.
   */
  void returnDbConn(DbConn *const dbConn);

private:

  /**
   * The database login details to be used to create new connections.
   */
  DbLogin m_dbLogin;

  /**
   * The database connection factory for the pool.
   */
  std::unique_ptr<DbConnFactory> m_dbConnFactory;

  /**
   * The number of database connections within the pool.
   */
  uint64_t m_nbDbConns;

  /**
   * Mutex used to serialize access to the database connections within the pool.
   */
  std::mutex m_dbConnsMutex;

  /**
   * Condition variable used by threads returning connections to the pool to
   * notify threads waiting for connections.
   */
  std::condition_variable m_dbConnsCv;

  /**
   * The database connections within the pool.
   */
  std::list<DbConn *> m_dbConns;

  /**
   * Creates the database connection factory for the pool based on the specified
   * login details.
   *
   * @param dbLogin The database login details to be used to create new
   * connections.
   */
  void createDbConnFactory(const DbLogin &dbLogin);

  /**
   * Creates the specified number of database connections with the pool.
   *
   * @param nbDbConns The number of database connections to be created.
   */
  void createDbConns(const uint64_t nbDbConns);

}; // class DbConnPool

} // namespace rdbms
} // namespace cta
