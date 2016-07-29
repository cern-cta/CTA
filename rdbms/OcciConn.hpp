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

#include <occi.h>
#include <mutex>

namespace cta {
namespace rdbms {

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
  ~OcciConn() throw() override;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close() override;

  /**
   * Returns the underlying OCCI connection.
   *
   * This method will always return a valid pointer.
   *
   * @return The underlying OCCI connection.
   */
  oracle::occi::Connection *get() const;

  /**
   * An alias for the get() method.
   *
   * @return The underlying OCCI connection.
   */
  oracle::occi::Connection *operator->() const;

  /**
   * Creates a prepared statement.
   *
   * @param sql The SQL statement.
   * @param autocommitMode The autocommit mode of the statement.
   * @return The prepared statement.
   */
  virtual std::unique_ptr<Stmt> createStmt(const std::string &sql, const Stmt::AutocommitMode autocommitMode) override;

  /**
   * Commits the current transaction.
   */
  virtual void commit() override;

  /**
   * Rolls back the current transaction.
   */
  virtual void rollback() override;

private:

  /**
   * Mutex used to serialize access to this object.
   */
  std::mutex m_mutex;

  /**
   * The OCCI environment.
   */
  oracle::occi::Environment *m_env;

  /**
   * The OCCI connection.
   */
  oracle::occi::Connection *m_conn;

}; // class OcciConn

} // namespace rdbms
} // namespace cta
