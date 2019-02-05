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
#include "rdbms/Rset.hpp"
#include "common/optional.hpp"

#include <list>
#include <memory>
#include <mutex>

namespace cta {
namespace rdbms {

namespace wrapper {
  class Stmt;
}

class StmtPool;

/**
 * A smart database statement that will automatically return the underlying
 * database statement to its parent database connection when it goes out of
 * scope.
 */
class Stmt {
public:

  /**
   * Constructor.
   */
  Stmt();

  /**
   * Constructor.
   *
   * @param stmt The database statement.
   * @param stmtPool The database statement pool to which the m_stmt should be returned.
   */
  Stmt(std::unique_ptr<wrapper::Stmt> stmt, StmtPool &stmtPool);

  /**
   * Deletion of the copy constructor.
   */
  Stmt(Stmt &) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object.
   */
  Stmt(Stmt &&other);

  /**
   * Destructor.
   *
   * Returns the database statement back to its connection.
   */
  ~Stmt() noexcept;

  /**
   * Deletion of the copy assignment operator.
   */
  Stmt &operator=(const Stmt &) = delete;

  /**
   * Move assignment operator.
   *
   * @param rhs The object on the right-hand side of the operator.
   * @return This object.
   */
  Stmt &operator=(Stmt &&rhs);

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  const std::string &getSql() const;

  /**
   * Returns the index of the specified SQL parameter.
   *
   * @param paramName The name of the SQL parameter.
   * @return The index of the SQL parameter.
   */
  uint32_t getParamIdx(const std::string &paramName) const;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint64(const std::string &paramName, const uint64_t paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindOptionalUint64(const std::string &paramName, const optional<uint64_t> &paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindBool(const std::string &paramName, const bool paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindOptionalBool(const std::string &paramName, const optional<bool> &paramValue);

  /** 
   * Binds an SQL parameter of type string.
   *
   * Please note that this method will throw an exception if the string
   * parameter is empty.  If a null value is to be bound then the
   * bindOptionalString() method should be used.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bindString(const std::string &paramName, const std::string &paramValue);

  /** 
   * Binds an SQL parameter of type optional-string.
   *
   * Please note that this method will throw an exception if the optional string
   * parameter has the empty string as its value.  An optional string parameter
   * should either have a non-empty string value or no value at all.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bindOptionalString(const std::string &paramName, const optional<std::string> &paramValue);

  /**
   *  Executes the statement and returns the result set.
   *
   *  @param autocommitMode The autocommit mode of the statement.
   *  @return The result set.
   */
  Rset executeQuery(const AutocommitMode autocommitMode = AutocommitMode::AUTOCOMMIT_ON);

  /**
   * Executes the statement.
   *
   *  @param autocommitMode The autocommit mode of the statement.
   */
  void executeNonQuery(const AutocommitMode autocommitMode = AutocommitMode::AUTOCOMMIT_ON);

  /**
   * Returns the number of rows affected by the last execution of this
   * statement.
   *
   * @return The number of affected rows.
   */
  uint64_t getNbAffectedRows() const;

  /**
   * Returns a reference to the underlying statement object that is not pool
   * aware.
   */
  wrapper::Stmt &getStmt();

private:

  /**
   * The database statement.
   */
  std::unique_ptr<wrapper::Stmt> m_stmt;

  /**
   * The database statement pool to which the m_stmt should be returned.
   */
  StmtPool *m_stmtPool;

}; // class Stmt

} // namespace rdbms
} // namespace cta
