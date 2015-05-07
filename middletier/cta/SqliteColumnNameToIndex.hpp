/**
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

#include <map>
#include <sqlite3.h>
#include <string>

namespace cta {

/**
 * Functor that takes the name of a column of an Sqlite statement and returns
 * the corresponding index.
 */
class SqliteColumnNameToIndex {
public:

  /**
   * Constructor.
   *
   * @param statement The Sqlite statment.
   */
  SqliteColumnNameToIndex(sqlite3_stmt *const statement);

  /**
   * Returns the index of the specified column.
   *
   * @param columnName The name of the column.
   */
  int operator()(const std::string &columnName) const;

private:

  /**
   * The mapping from column name to column index.
   */
  std::map<std::string, int> m_columnNameToIndex;

}; // class SqliteColumnNameToIndex

} // namespace cta
