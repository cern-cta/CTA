/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/process/threading/CondVar.hpp"
#include "common/process/threading/Mutex.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "rdbms/Stmt.hpp"

#include <iostream>
#include <map>
#include <memory>
#include <stdint.h>
#include <vector>

namespace cta::rdbms {

namespace wrapper {
class ConnWrapper;
class StmtWrapper;
}  // namespace wrapper

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
  Stmt getStmt(wrapper::ConnWrapper& conn, const std::string& sql);

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
  std::map<std::string, std::vector<std::unique_ptr<wrapper::StmtWrapper>>> m_stmts;

};  // class StmtPool

}  // namespace cta::rdbms
