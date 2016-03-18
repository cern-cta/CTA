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

#include <mutex>
#include <sqlite3.h>
#include <string>

namespace cta {
namespace catalogue {

/**
 * Forward declaraion to avoid a circular dependency beween SqliteConn and
 * SqliteStmt.
 */
class SqliteStmt;

/**
 * A convenience wrapper around a connection to an SQLite database.
 */
class SqliteConn {
public:

  /**
   * Constructor.
   *
   * @param filename The filename to be passed to the sqlite3_open() function.
   */
  SqliteConn(const std::string &filename);

  /**
   * Destructor.
   */
  ~SqliteConn() throw();

  /**
   * Idempotent close() method.  The destructor calls this method.
   */
  void close();

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

  /**
   * Creates a prepared statement.
   *
   * @sql The SQL statement.
   * @return The prepared statement.
   */
  SqliteStmt *createStmt(const std::string &sql);

private:

  /**
   * Mutex used to serialize access to the database connection.
   */
  std::mutex m_mutex;

  /**
   * The database connection.
   */
  sqlite3 *m_conn;

}; // class SqliteConn

} // namespace catalogue
} // namespace cta
