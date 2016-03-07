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

#include <sqlite3.h>
#include <string>

namespace cta {
namespace catalogue {

/**
 * A C++ wrapper around a connectioin an SQLite database.
 */
class SqliteConn {
public:

  /**
   * Constructor.
   *
   * @param filename The filename to be passed to the sqlit3_open() function.
   */
  SqliteConn(const std::string &filename);

  /**
   * Destructor.
   */
  ~SqliteConn() throw();

  /**
   * Returns the underlying database connection.
   *
   * @return the underlying database connection.
   */
  sqlite3 *get() const;

  /**
   * Enables foreign key constraints.
   */
  void enableForeignKeys();

  /**
   * Executes the specified non-query SQL statement.
   *
   * @param sql The SQL statement.
   */
  void execNonQuery(const std::string &sql);

private:

  /**
   * The database connection.
   */
  sqlite3 *m_conn;

}; // class SqlLiteConn

} // namespace catalogue
} // namespace cta
