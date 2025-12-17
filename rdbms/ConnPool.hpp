/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/process/threading/CondVar.hpp"
#include "common/process/threading/Mutex.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnAndStmts.hpp"
#include "rdbms/wrapper/ConnFactory.hpp"
#include "rdbms/wrapper/ConnWrapper.hpp"

#include <list>
#include <memory>

namespace cta::rdbms {

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
  ConnPool(const Login& login, const uint64_t maxNbConns);

  ~ConnPool();

  CTA_GENERATE_EXCEPTION_CLASS(ConnPoolConfiguredWithZeroConns);

  /**
   * Takes a connection from the pool.
   *
   * Please note that this method will block if the pool is empty.  In such a
   * situation this method will unblock when a connection is returned to the
   * pool.  There is one exception to this rule.  If the pool was configured
   * with maxNbConns set to 0 then calling this method with throw a
   * ConnPoolConfiguredWithZeroConns exception.
   *
   * @return A connection from the pool.
   * @throw ConnPoolConfiguredWithZeroConns If this pool was configured with
   * maxNbConns set to 0.
   */
  Conn getConn();

  uint64_t getNbConnsOnLoan() const { return m_nbConnsOnLoan; }

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
   * Adds a number of connections on loan.
   */
  void addNbConnsOnLoan(uint64_t nbConns);

  /**
   * Adds a number of connections on loan.
   */
  void removeNbConnsOnLoan(uint64_t nbConns);
  /**
   * Resets the number of connections on loan to 0.
   * Noexcept so that this can be used in a destructor.
   */
  void resetNbConnsOnLoan() noexcept;

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
  uint64_t m_nbConnsOnLoan = 0;

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
  std::list<std::unique_ptr<ConnAndStmts>> m_connsAndStmts;
};  // class ConnPool

}  // namespace cta::rdbms
