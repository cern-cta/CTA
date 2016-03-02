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

#include "catalogue/SqliteConn.hpp"

#include <sqlite3.h>
#include <string>

namespace cta {
namespace catalogue {

/**
 * A C++ wrapper around an SQLite prepared statement.
 */
class SqliteStmt {
public:

  /**
   * Constructor.
   *
   * @param conn The SQLite database connection.
   */
  SqliteStmt(sqlite3 *const conn);

  /**
   * Constructor.
   *
   * @param conn The SQLite database connection.
   */
  SqliteStmt(SqliteConn &conn);

  /**
   * Destructor.
   */
  ~SqliteStmt() throw();

  /**
   * Returns the underlying prepared statement.
   *
   * @return the underlying prepared statemen.
   */
  sqlite3_stmt *get();

private:

  /**
   * The database statement.
   */
  sqlite3_stmt *m_stmt;

}; // class SqlLiteStmt

} // namespace catalogue
} // namespace cta
