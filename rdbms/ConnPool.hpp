/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/threading/CondVar.hpp"
#include "common/threading/Mutex.hpp"
#include "rdbms/ConnAndStmts.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/wrapper/ConnWrapper.hpp"
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
  ConnPool(const Login& login, const uint64_t maxNbConns);

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
  std::list<std::unique_ptr<ConnAndStmts>> m_connsAndStmts;
};  // class ConnPool

}  // namespace rdbms
}  // namespace cta
