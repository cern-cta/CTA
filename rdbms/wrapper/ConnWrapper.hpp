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

#include "rdbms/AutocommitMode.hpp"
#include "rdbms/wrapper/StmtWrapper.hpp"

#include <atomic>
#include <list>
#include <memory>
#include <string>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * Abstract class that specifies the interface to a database connection.
 */
class ConnWrapper {
public:

  /**
   * Destructor.
   */
  virtual ~ConnWrapper() = 0;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close() = 0;

  /**
   * Sets the autocommit mode of the connection.
   *
   * @param autocommitMode The autocommit mode of the connection.
   * @throw AutocommitModeNotSupported If the specified autocommit mode is not
   * supported.
   */
  virtual void setAutocommitMode(const AutocommitMode autocommitMode) = 0;

  /**
   * Returns the autocommit mode of the connection.
   *
   * @return The autocommit mode of the connection.
   */
  virtual AutocommitMode getAutocommitMode() const noexcept = 0;

  /**
   * Executes the statement.
   *
   * @param sql The SQL statement.
   */
  virtual void executeNonQuery(const std::string &sql) = 0;

  /**
   * Creates a prepared statement.
   *
   * @param sql The SQL statement.
   * @return The prepared statement.
   */
  virtual std::unique_ptr<StmtWrapper> createStmt(const std::string &sql) = 0;

  /**
   * Commits the current transaction.
   */
  virtual void commit() = 0;

  /**
   * Rolls back the current transaction.
   */
  virtual void rollback() = 0;

  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  virtual std::list<std::string> getTableNames() = 0;

  /**
   * Returns true if this connection is open.
   */
  virtual bool isOpen() const = 0;

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
  virtual std::list<std::string> getSequenceNames()  = 0;

}; // class ConnWrapper

} // namespace wrapper
} // namespace rdbms
} // namespace cta
