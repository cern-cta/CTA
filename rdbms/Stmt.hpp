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

#include "rdbms/AutocommitMode.hpp"
#include "rdbms/Rset.hpp"
#include "common/optional.hpp"

#include <list>
#include <memory>
#include <mutex>

namespace cta {
namespace rdbms {

namespace wrapper {
  class StmtWrapper;
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
  Stmt(std::unique_ptr<wrapper::StmtWrapper> stmt, StmtPool &stmtPool);

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
   * Reset the statment by returning any owned statement back to its statment
   * pool.
   *
   * This method is idempotent.
   */
  void reset() noexcept;

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
  void bindUint8(const std::string &paramName, const optional<uint8_t> &paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint16(const std::string &paramName, const optional<uint16_t> &paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint32(const std::string &paramName, const optional<uint32_t> &paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindUint64(const std::string &paramName, const optional<uint64_t> &paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindDouble(const std::string &paramName, const optional<double> &paramValue);

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bindBool(const std::string &paramName, const optional<bool> &paramValue);

  /** 
   * Binds an SQL parameter of type binary blob.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bindBlob(const std::string &paramName, const std::string &paramValue);

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
  void bindString(const std::string &paramName, const optional<std::string> &paramValue);

  /**
   *  Executes the statement and returns the result set.
   *
   *  @return The result set.
   */
  Rset executeQuery();

  /**
   * Executes the statement.
   */
  void executeNonQuery();

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
  wrapper::StmtWrapper &getStmt();

private:

  /**
   * The database statement.
   */
  std::unique_ptr<wrapper::StmtWrapper> m_stmt;

  /**
   * The database statement pool to which the m_stmt should be returned.
   */
  StmtPool *m_stmtPool;

}; // class Stmt

} // namespace rdbms
} // namespace cta
