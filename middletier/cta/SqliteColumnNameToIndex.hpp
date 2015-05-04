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
