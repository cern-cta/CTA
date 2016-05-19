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

#include "catalogue/DbRset.hpp"

#include <stdint.h>

namespace cta {
namespace catalogue {

/**
 * Abstract class specifying the interface to a database statement.
 *
 * Please note that this interface intentionally uses C-strings instead of
 * std::string so that it can be used by code compiled against the CXX11 ABI and
 * by code compiled against a pre-CXX11 ABI.
 */
class DbStmt {
public:

  /**
   * Destructor.
   */
  virtual ~DbStmt() throw() = 0;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close() = 0;

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  virtual const char *getSql() const = 0;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  virtual void bind(const char *const paramName, const uint64_t paramValue) = 0;

  /** 
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  virtual void bind(const char*paramName, const char *paramValue) = 0;

  /**
   *  Executes the statement and returns the result set.
   *
   *  @return The result set.  Please note that it is the responsibility of the
   *  caller to free the memory associated with the result set.
   */
  virtual DbRset *executeQuery() = 0;

}; // class DbStmt

} // namespace catalogue
} // namespace cta
