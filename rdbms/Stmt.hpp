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

#include "common/optional.hpp"
#include "rdbms/Rset.hpp"

#include <memory>
#include <stdint.h>
#include <string>

namespace cta {
namespace rdbms {

/**
 * Abstract class specifying the interface to a database statement.
 */
class Stmt {
public:

  /**
   * A statement can either have auto commiting mode turned on or off.
   */
  enum class AutocommitMode {
    ON,
    OFF
  };

  /**
   * Constructor.
   *
   * @param autocommitMode The autocommit mode of the statement.
   */
  Stmt(const AutocommitMode autocommitMode);

  /**
   * Returns the autocommit mode of teh statement.
   *
   * @return The autocommit mode of teh statement.
   */
  AutocommitMode getAutoCommitMode() const noexcept;

  /**
   * Destructor.
   */
  virtual ~Stmt() throw() = 0;

  /**
   * Deletion of the copy constructor.
   */
  Stmt(Stmt &) = delete;

  /**
   * Deletion of the move constructor.
   */
  Stmt(Stmt &&) = delete;

  /**
   * Deletion of the copy assignment operator.
   */
  Stmt &operator=(const Stmt &) = delete;

  /**
   * Deletion of the move assignment operator.
   */
  Stmt &operator=(Stmt &&) = delete;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close() = 0;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  virtual const std::string &getSql() const = 0;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindUint64(const std::string &paramName, const uint64_t paramValue) = 0;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bindOptionalUint64(const std::string &paramName, const optional<uint64_t> &paramValue) = 0;

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
  virtual void bindString(const std::string &paramName, const std::string &paramValue) = 0;

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
  virtual void bindOptionalString(const std::string &paramName, const optional<std::string> &paramValue) = 0;

  /**
   *  Executes the statement and returns the result set.
   *
   *  @return The result set.  Please note that it is the responsibility of the
   *  caller to free the memory associated with the result set.
   */
  virtual std::unique_ptr<Rset> executeQuery() = 0;

  /**
   * Executes the statement.
   */
  virtual void executeNonQuery() = 0;

  /**
   * Returns the number of rows affected by the last execution of this
   * statement.
   *
   * @return The number of affected rows.
   */
  virtual uint64_t getNbAffectedRows() const = 0;

protected:

  /**
   * The autocommit mode of the statement.
   */
  AutocommitMode m_autoCommitMode;

}; // class Stmt

} // namespace rdbms
} // namespace cta
