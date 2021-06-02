/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "rdbms/StmtPool.hpp"
#include "rdbms/wrapper/ConnWrapper.hpp"

#include <iostream>
#include <memory>

namespace cta {
namespace rdbms {

/**
 * Class to enforce prepared statements are destroyed before their corresponding
 * database connection.
 */
struct ConnAndStmts {

  /**
   * Constructor.
   */
  ConnAndStmts() {
  }

  /**
   * Deletion of the copy constructor.
   */
  ConnAndStmts(ConnAndStmts &) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object.
   */
  ConnAndStmts(ConnAndStmts &&other):
    conn(std::move(other.conn)),
    stmtPool(std::move(other.stmtPool)) {
  }

  /**
   * Equality operator.
   *
   * @param rhs The object on the right hand side of the operator.
   * @return True if equal.
   */
  bool operator==(const ConnAndStmts &rhs) {
    return conn.get() == rhs.conn.get();
  }

  /**
   * Inequality operator.
   *
   * @param rhs The object on the right hand side of the operator.
   * @return True if not equal.
   */
  bool operator!=(const ConnAndStmts &rhs) {
    return !operator==(rhs);
  }

  /**
   * The database connection.
   *
   * The database connection must be destroyed after all of its corresponding
   * prepared statements.  This means the conn member-variable must be declared
   * before the stmtPool member-variable.
   */
  std::unique_ptr<wrapper::ConnWrapper> conn;

  /**
   * Pool of prepared statements.
   *
   * The prepared statements must be destroyed before their corresponding
   * database connection.  This means the stmtPool member-variable must be
   * declared after the conn member-variable.
   */
  std::unique_ptr<StmtPool> stmtPool;

}; // class ConnAndStmts

} // namespace rdbms
} // namespace cta
