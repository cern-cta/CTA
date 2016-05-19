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

#include "catalogue/DbStmt.hpp"

namespace cta {
namespace catalogue {

/**
 * Abstract class that specifies the interface to a database connection.
 *
 * Please note that this interface intentionally uses C-strings instead of
 * std::string so that it can be used by code compiled against the CXX11 ABI and
 * by code compiled against a pre-CXX11 ABI.
 */
class DbConn {
public:

  /**
   * Destructor.
   */
  virtual ~DbConn() throw() = 0;

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  virtual void close() = 0;

  /**
   * Creates a prepared statement.
   *
   * This method will throw an exception if the sql parameter is a NULL pointer.
   *
   * @sql The SQL statement.
   * @return The prepared statement.
   */
  virtual DbStmt *createStmt(const char *const sql) = 0;

}; // class DbConn

} // namespace catalogue
} // namespace cta
