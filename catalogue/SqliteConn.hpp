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

#include "catalogue/DbConn.hpp"

#include <mutex>
#include <sqlite3.h>

namespace cta {
namespace catalogue {

/**
 * Forward declaration to avoid a circular dependency between SqliteConn and
 * SqliteStmt.
 */
class SqliteStmt;

/**
 * A convenience wrapper around a connection to an SQLite database.
 */
class SqliteConn: public DbConn {
public:

  /**
   * The SqliteStmt class can lock the m_mutex member of the SqliteConn class
   * and it can read the pointer to the SQLite connection.
   */
  friend SqliteStmt;

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
  virtual void close();

  /**
   * Creates a prepared statement.
   *
   * @sql The SQL statement.
   * @return The prepared statement.
   */
  virtual DbStmt *createStmt(const std::string &sql);

  /**
   * Commits the current transaction.
   */
  virtual void commit();

  /**
   * Rolls back the current transaction.
   */
  virtual void rollback();

  /**
   * This ia an SqliteConn specific method that prints the database schema to
   * the specified output stream.
   *
   * Please note that this method is intended to be used for debugging.  The
   * output is subjectively not pretty.
   *
   * @param os The output stream.
   */
  void printSchema(std::ostream &os);

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
