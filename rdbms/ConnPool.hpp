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

#include "common/threading/CondVar.hpp"
#include "common/threading/Mutex.hpp"
#include "rdbms/ConnAndStmts.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/wrapper/Conn.hpp"
#include "rdbms/wrapper/ConnFactory.hpp"

#include <list>
#include <memory>

namespace cta {
namespace rdbms {

class Login;

/**
 * A pool of database connections.
 */
class ConnPool {
public:

  /**
   * Constructor.
   *
   * @param login The database login details to be used to create new
   * connections.
   * @param maxNbConns The maximum number of database connections within the
   * pool.
   */
  ConnPool(const Login &login, const uint64_t maxNbConns);

  /**
   * Takes a connection from the pool.
   *
   * Please note that this method will block if the pool is empty.  In such a
   * situation this method will unblock when a connection is returned to the
   * pool.
   *
   * @return A connection from the pool.
   */
  Conn getConn();

private:

  friend Conn;

  /**
   * If the specified database connection is open, then this method calls
   * commit on the connection and returns it to the pool.
   *
   * If the specified database connection is open, then this method sets the
   * autocommit mode of the connection to AUTOCOMMIT_ON because this is the
   * default value of a newly created connection.
   *
   * If the specified database connection is closed, then this method closes all
   * connections within the pool.
   *
   * A closed connection is reopened when it is pulled from the pool.
   *
   * @param connAndStmts The connection to be commited and returned to the pool.
   */
  void returnConn(std::unique_ptr<ConnAndStmts> connAndStmts);

  /**
   * The database connection factory.
   */
  std::unique_ptr<wrapper::ConnFactory> m_connFactory;

  /**
   * The maximum number of database connections within the pool.
   */
  uint64_t m_maxNbConns;

  /**
   * The number of database connections currently on loan.
   */
  uint64_t m_nbConnsOnLoan;

  /**
   * Mutex used to serialize access to the database connections within the pool.
   */
  threading::Mutex m_connsAndStmtsMutex;

  /**
   * Condition variable used by threads returning connections to the pool to
   * notify threads waiting for connections.
   */
  threading::CondVar m_connsAndStmtsCv;

  /**
   * The database connections within the pool.
   */
  std::list<std::unique_ptr<ConnAndStmts> > m_connsAndStmts;

  /**
   * Creates the specified number of database connections with the pool.
   *
   * @param nbConns The number of database connections to be created.
   */
  void createConns(const uint64_t nbConns);

}; // class ConnPool

} // namespace rdbms
} // namespace cta
