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

#include "catalogue/ColumnNameToIdx.hpp"

#include <map>
#include <stdint.h>
#include <sqlite3.h>
#include <string>

namespace cta {
namespace catalogue {

/**
 * Forward declaration to avoid a circular dependency between SqliteRset and
 * SqliteStmt.
 */
class SqliteStmt;

/**
 * A convenience wrapper around an SQLite prepared statement that is either in
 * the SQLITE_ROW or SQLITE_DONE states.
 */
class SqliteStmt {
public:

  /**
   * Constructor.
   *  
   * @param stmt The prepared statement.
   */
  SqliteRset(SqliteStmt &stmt);

  /**
   * Destructor.
   */
  ~SqliteRset() throw();

  /**
   * Returns the SQL statement.
   *
   * @return The SQL statement.
   */
  const std::string &getSql() const;

  /**
   * Returns the underlying prepared statement.
   *
   * @return the underlying prepared statemen.
   */
  sqlite3_stmt *get() const;

  /**
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */
  void bind(const std::string &paramName, const uint64_t paramValue);

  /** 
   * Binds an SQL parameter.
   *
   * @param paramName The name of the parameter.
   * @param paramValue The value to be bound.
   */ 
  void bind(const std::string &paramName, const std::string &paramValue);

  /**
   * Convenience wrapper around sqlite3_step().
   *
   * This method throws a exception if sqlite3_step() returns an error.
   *
   * @return See sqlite3_step() documentation.
   */
  int step();

  /**
   * Returns a map from column name to column index.
   *
   * @return a map from column name to column index.
   */
  ColumnNameToIdx getColumnNameToIdx() const;

  /**
   * Convenience wrapper around sqlite3_column_text().
   *
   * If sqlite3_column_text() returns NULL then this method returns an empty
   * string.
   *
   * @param colIdx The index of the column.
   * @return The value of the specified column.
   */
  std::string columnText(const int colIdx);

  /**
   * Returns true if the specified text column contains a null value.
   *
   * @param colIdx The index of the column.
   * @return True if the column value is null.
   */
  bool columnTextIsNull(const int colIdx);

  /**
   * Convenience wrapper around sqlite3_column_int64().
   *
   * @param colIdx The index of the column.
   * @return The value of the specified column.
   */
  uint64_t columnUint64(const int colIdx);

private:

  /**
   * Mutex used to serialize access to the prepared statement.
   */
  std::mutex m_mutex;

  /**
   * The SQL statement.
   */
  const std::string m_sql;

  /**
   * The prepared statement.
   */
  sqlite3_stmt *m_stmt;

  /**
   * Returns the index of the SQL parameter with the specified name.
   *
   * This method throws an exception if the parameter is not found.
   */
  int getParamIndex(const std::string &paramName);

}; // class SqlLiteStmt

} // namespace catalogue
} // namespace cta
