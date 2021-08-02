/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "common/threading/CondVar.hpp"
#include "common/threading/Mutex.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "rdbms/Stmt.hpp"

#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <stdint.h>

namespace cta {
namespace rdbms {

namespace wrapper {
  class ConnWrapper;
  class StmtWrapper;
}

/**
 * A pool of prepared database statements.
 */
class StmtPool {
public:

  /**
   * Takes a database statement from the pool if one is present else a new
   * statement will be prepared.
   *
   * @param conn The database connection to which the database statements
   * @param sql The SQL statement.
   * @return The prepared statement.
   */
  Stmt getStmt(wrapper::ConnWrapper &conn, const std::string &sql);

  /**
   * Returns the number of cached statements currently in the pool.
   *
   * @return The number of cached statements currently in the pool.
   */
  uint64_t getNbStmts() const;

  /**
   * Clears the pooll of prepared statements which includes destroying those
   * statements.
   */
  void clear();

private:

  friend Stmt;

  /**
   * Returns the specified statement to the pool.
   *
   * @param stmt The database statement to be returned to the pool.
   */
  void returnStmt(std::unique_ptr<wrapper::StmtWrapper> stmt);

  /**
   * Mutex used to serialize access to the database statements within the pool.
   */
  mutable threading::Mutex m_stmtsMutex;

  /**
   * The cached database statements.
   *
   * Please note that for a single key there maybe more than one cached
   * statement, hence the map to list of statements.
   */
  std::map<std::string, std::list< std::unique_ptr<wrapper::StmtWrapper> > > m_stmts;

}; // class StmtPool

} // namespace rdbms
} // namespace cta
