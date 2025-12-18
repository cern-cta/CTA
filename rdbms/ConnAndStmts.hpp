/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/StmtPool.hpp"
#include "rdbms/wrapper/ConnWrapper.hpp"

#include <iostream>
#include <memory>

namespace cta::rdbms {

/**
 * Class to enforce prepared statements are destroyed before their corresponding database connection
 */
struct ConnAndStmts {
  /**
   * Constructor
   */
  ConnAndStmts() = default;

  /**
   * Deletion of the copy constructor
   */
  ConnAndStmts(ConnAndStmts&) = delete;

  /**
   * Move constructor
   *
   * @param other The other object
   */
  ConnAndStmts(ConnAndStmts&& other) noexcept : conn(std::move(other.conn)), stmtPool(std::move(other.stmtPool)) {}

  /**
   * Equality operator
   *
   * @param rhs The object on the right hand side of the operator.
   * @return True if equal.
   */
  bool operator==(const ConnAndStmts& rhs) { return conn.get() == rhs.conn.get(); }

  /**
   * Inequality operator
   *
   * @param rhs The object on the right hand side of the operator.
   * @return True if not equal.
   */
  bool operator!=(const ConnAndStmts& rhs) { return !operator==(rhs); }

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
};

}  // namespace cta::rdbms
