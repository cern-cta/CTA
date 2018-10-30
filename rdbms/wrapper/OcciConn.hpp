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

#include "common/threading/MutexLocker.hpp"
#include "rdbms/wrapper/Conn.hpp"

#include <occi.h>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * Forward declaraion to avoid a circular dependency beween OcciConn and
 * OcciStmt.
 */
class OcciStmt;

/**
 * A convenience wrapper around a connection to an OCCI database.
 */
class OcciConn: public Conn {
public:

  /**
   * Constructor.
   *
   * This method will throw an exception if the OCCI connection is a nullptr
   * pointer.
   *
   * @param env The OCCI environment.
   * @param conn The OCCI connection.
   */
  OcciConn(oracle::occi::Environment *env, oracle::occi::Connection *const conn);

  /**
   * Destructor.
   */
  ~OcciConn() override;

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
  std::unique_ptr<Stmt> createStmt(const std::string &sql, const AutocommitMode autocommitMode) override;

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

  /**
   * Returns the names of all the sequences in the database schema in
   * alphabetical order.
   *
   * If the underlying database technologies does not supported sequences then
   * this method simply returns an empty list.
   *
   * @return The names of all the sequences in the database schema in
   * alphabetical order.
   */
  std::list<std::string> getSequenceNames() override;

private:

  friend OcciStmt;

  /**
   * Mutex used to serialize access to this object.
   */
  threading::Mutex m_mutex;

  /**
   * The OCCI environment.
   */
  oracle::occi::Environment *m_env;

  /**
   * The OCCI connection.
   */
  oracle::occi::Connection *m_occiConn;

  /**
   * Closes the specified OCCI statement.
   *
   * @param stmt The OCCI statement to be closed.
   */
  void closeStmt(oracle::occi::Statement *const stmt);

}; // class OcciConn

} // namespace wrapper
} // namespace rdbms
} // namespace cta
