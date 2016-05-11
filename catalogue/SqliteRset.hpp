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

#include <memory>
#include <stdint.h>
#include <sqlite3.h>

namespace cta {
namespace catalogue {

/**
 * Forward declaration.
 */
class SqliteStmt;

/**
 * The result set of an sql query.
 */
class SqliteRset {
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
  const char *getSql() const;

  /**
   * Attempts to get the next row of the result set.
   *
   * @return True if a row has been retrieved else false if there are no more
   * rows in the result set.
   */
  bool next();

  /**
   * Returns the value of the specified column as a string.
   *
   * Please note that a C string is returned instead of an std::string so that
   * this method can be used by code compiled against the CXX11 ABI and by code
   * compiled against a pre-CXX11 ABI.
   *
   * Please note that if the value of the column is NULL within the database
   * then a NULL pointer is returned.
   *
   * @param colName The name of the column.
   * @return The string value of the specified column or NULL if the value of
   * the column within the database is NULL.  Please note that it is the
   * responsibility of the caller to free the memory associated with the string
   * using delete[] operator.
   */
  const char *columnText(const char *const colName) const;

  /**
   * Returns the value of the specified column as an integer.
   *
   * @param colName The name of the column.
   * @return The value of the specified column.
   */
  uint64_t columnUint64(const char *const colName) const;

private:

  /**
   * The prepared statement.
   */
  SqliteStmt &m_stmt;

  /**
   * True if the next() method has not yet been called.
   */
  bool m_nextHasNotBeenCalled;

  /**
   * Forward declaration of the nested class ColumnNameIdx that is intentionally
   * hidden in the cpp file of the SqliteRset class.  The class is hidden in
   * order to enable the SqliteRset class to be used by code compiled against
   * the CXX11 ABI and used by code compiled against the pre-CXX11 ABI.
   */
  class ColumnNameToIdx;

  /**
   * Map from column name to column index.
   *
   * Please note that the type of the map is intentionally forward declared in
   * order to avoid std::string being used.  This is to aid with working with
   * pre and post CXX11 ABIs.
   */
  std::unique_ptr<ColumnNameToIdx> m_colNameToIdx;

  /**
   * Populates the map from column name to column index.
   */
  void populateColNameToIdxMap();

}; // class SqlLiteStmt

} // namespace catalogue
} // namespace cta
