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

#include "cta/Exception.hpp"
#include "cta/SqliteColumnNameToIndex.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::SqliteColumnNameToIndex::SqliteColumnNameToIndex(
  sqlite3_stmt *const statement) {
  const int colCount = sqlite3_column_count(statement);
  if(colCount==0) {
    std::ostringstream message;
    message << "SqliteColumnNameToIndex() - "
      "SQLite error: sqlite3_column_count() returned 0";
    throw(cta::Exception(message.str()));
  }
  for(int i=0; i<colCount; i++) {
    const char *const columnName = sqlite3_column_origin_name(statement,i);
    if(columnName == NULL) {
      std::ostringstream message;
      message << "SqliteColumnNameToIndex() -"
        " SQLite error: sqlite3_column_origin_name() returned NULL";
      throw(cta::Exception(message.str()));
    }
    m_columnNameToIndex[std::string(columnName)] = i;
  }
}

//------------------------------------------------------------------------------
// operator()
//------------------------------------------------------------------------------
int cta::SqliteColumnNameToIndex::operator()(const std::string &columnName)
  const {
  std::map<std::string, int>::const_iterator it =
    m_columnNameToIndex.find(columnName);
  if(it != m_columnNameToIndex.end()) {
    return it->second;
  } else {
    std::ostringstream message;
    message << "SqliteColumnNameToIndex() - column " << columnName <<
      " does not exist";
    throw cta::Exception(message.str());
  }
}
