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

#include "catalogue/Sqlite.hpp"
#include "catalogue/SqliteRset.hpp"
#include "catalogue/SqliteStmt.hpp"

#include <cstring>
#include <sstream>
#include <stdexcept>


namespace cta {
namespace catalogue {

/**
 * A map from column name to column index.
 *
 * Please note that this class is intentionally hidden within this cpp file to
 * enable the SqliteRset class to be used by code compiled against the CXX11 ABI
 * and by code compiled against a pre-CXX11 ABI.
 */
class SqliteRset::ColumnNameToIdx {
public:

  /**
   * Adds the specified column name to index mapping.
   *
   * This method throws an exception if the specified column name is a
   * duplicate, in other words has already been added to the map.
   *
   * @param name The name of the column.
   * @param idx The index of the column.
   */
  void add(const std::string &name, const int idx) {
    if(m_nameToIdx.end() != m_nameToIdx.find(name)) {
      throw std::runtime_error(std::string(__FUNCTION__) + " failed: " + name + " is a duplicate");
    }
    m_nameToIdx[name] = idx;
  }

  /**
   * Returns the index of the column with the specified name.
   *
   * This method throws an exception if the specified column name is not in the
   * map.
   *
   * @return the index of the column with the specified name.
   */
  int getIdx(const std::string &name) const {
    auto it = m_nameToIdx.find(name);
    if(m_nameToIdx.end() == it) {
      throw std::runtime_error(std::string(__FUNCTION__) + " failed: Unknown column name " + name);
    }
    return it->second;
  }

  /**
   * Alias for the getIdx() method.
   *
   * @return the index of the column with the specified name.
   */
  int operator[](const std::string &name) const {
    return getIdx(name);
  }

  /**
   * Returns true if this map is empty.
   *
   * @return True if this map is empty.
   */
  bool empty() const {
    return m_nameToIdx.empty();
  }

private:

  /**
   * The underlying STL map from column name to column index.
   */
  std::map<std::string, int> m_nameToIdx;

}; // class SqliteRset::ColumnNameToIdx

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteRset::SqliteRset(SqliteStmt &stmt):
  m_stmt(stmt),
  m_nextHasNotBeenCalled(true) {
  m_colNameToIdx.reset(new ColumnNameToIdx());
}

//------------------------------------------------------------------------------
// destructor.
//------------------------------------------------------------------------------
SqliteRset::~SqliteRset() throw() {
  //m_colNameToIdx.release();
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const char *SqliteRset::getSql() const {
  return m_stmt.getSql();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool SqliteRset::next() {
  try {
    const int stepRc = sqlite3_step(m_stmt.get());

    // Throw an exception if the call to sqlite3_step() failed
    if(SQLITE_DONE != stepRc && SQLITE_ROW != stepRc) {
      throw std::runtime_error(Sqlite::rcToStr(stepRc));
    }

    if(m_nextHasNotBeenCalled) {
      m_nextHasNotBeenCalled = false;

      if(SQLITE_ROW == stepRc) {
        populateColNameToIdxMap();
      }
    }

    return SQLITE_ROW == stepRc;
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() +
      ": " + ne.what());
  }
}

//------------------------------------------------------------------------------
// populateColNameToIdxMap
//------------------------------------------------------------------------------
void SqliteRset::populateColNameToIdxMap() {
  try {
    const int nbCols = sqlite3_column_count(m_stmt.get());
    for (int i = 0; i < nbCols; i++) {
      const char *name = sqlite3_column_name(m_stmt.get(), i);
      if (NULL == name) {
        std::ostringstream msg;
        msg << "Failed to get column name for column index " << i;
        throw std::runtime_error(msg.str());
      }
      m_colNameToIdx->add(name, i);
    }
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: " + ne.what());
  }
}

//------------------------------------------------------------------------------
// columnText
//------------------------------------------------------------------------------
const char *SqliteRset::columnText(const char *const colName) const {
  const int colIdx = (*m_colNameToIdx)[colName];
  return (const char *)sqlite3_column_text(m_stmt.get(), colIdx);
}

//------------------------------------------------------------------------------
// columnUint64
//------------------------------------------------------------------------------
uint64_t SqliteRset::columnUint64(const char *const colName) const {
  const int colIdx = (*m_colNameToIdx)[colName];
  return (uint64_t)sqlite3_column_int64(m_stmt.get(), colIdx);
}

} // namespace catalogue
} // namespace cta
