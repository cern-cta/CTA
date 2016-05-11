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
 * A map from column name to column index and type.
 *
 * Please note that this class is intentionally hidden within this cpp file to
 * enable the SqliteRset class to be used by code compiled against the CXX11 ABI
 * and by code compiled against a pre-CXX11 ABI.
 */
class SqliteRset::ColNameToIdxAndType {
public:

  /**
   * Structure to store a column's index and type.  With SQLite 3 the type of a
   * column needs to be stored before any type conversion has taken place.  This
   * is because the result of calling the sqlite3_column_type() function is no
   * longer meaningful after such a conversion.
   */
  struct IdxAndType {
    /**
     * The index of the column.
     */
    int colIdx;

    /**
     * The type of the column as return by the sqlite3_column_type() function
     * before any type conversion has taken place.
     */
    int colType;

    /**
     * Constructor.  Set both member-variables to 0.
     */
    IdxAndType(): colIdx(0), colType(0) {
    }
  };

  /**
   * Adds the specified mapping from column name to column index and type.
   *
   * This method throws an exception if the specified column name is a
   * duplicate, in other words has already been added to the map.
   *
   * @param name The name of the column.
   * @param idxAndType The column index and type.
   */
  void add(const std::string &name, const IdxAndType &idxAndType) {
    if(m_nameToIdxAndType.end() != m_nameToIdxAndType.find(name)) {
      throw std::runtime_error(std::string(__FUNCTION__) + " failed: " + name + " is a duplicate");
    }
    m_nameToIdxAndType[name] = idxAndType;
  }

  /**
   * Returns the index and type of the column with the specified name.
   *
   * This method throws an exception if the specified column name is not in the
   * map.
   *
   * @param name The name of the column.
   * @return The index and type of the column.
   */
  IdxAndType getIdxAndType(const std::string &name) const {
    auto it = m_nameToIdxAndType.find(name);
    if(m_nameToIdxAndType.end() == it) {
      throw std::runtime_error(std::string(__FUNCTION__) + " failed: Unknown column name " + name);
    }
    return it->second;
  }

  /**
   * Alias for the getIdx() method.
   *
   * @return the index of the column with the specified name.
   */
  IdxAndType operator[](const std::string &name) const {
    return getIdxAndType(name);
  }

  /**
   * Returns true if this map is empty.
   *
   * @return True if this map is empty.
   */
  bool empty() const {
    return m_nameToIdxAndType.empty();
  }

private:

  /**
   * The underlying STL map from column name to column index.
   */
  std::map<std::string, IdxAndType> m_nameToIdxAndType;

}; // class SqliteRset::ColNameToIdx

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteRset::SqliteRset(SqliteStmt &stmt):
  m_stmt(stmt),
  m_nextHasNotBeenCalled(true) {
  m_columnNameToIdxAndType.reset(new ColNameToIdxAndType());
}

//------------------------------------------------------------------------------
// destructor.
//------------------------------------------------------------------------------
SqliteRset::~SqliteRset() throw() {
  //m_columnNameToIdxAndType.release();
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
        populateColNameToIdxAndTypeMap();
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
void SqliteRset::populateColNameToIdxAndTypeMap() {
  try {
    const int nbCols = sqlite3_column_count(m_stmt.get());
    for (int i = 0; i < nbCols; i++) {
      // Get the name of the column
      const char *colName = sqlite3_column_name(m_stmt.get(), i);
      if (NULL == colName) {
        std::ostringstream msg;
        msg << "Failed to get column name for column index " << i;
        throw std::runtime_error(msg.str());
      }

      // Get the type of the column
      ColNameToIdxAndType::IdxAndType idxAndType;
      idxAndType.colIdx = i;
      idxAndType.colType = sqlite3_column_type(m_stmt.get(), i);

      // Add the mapping from column name to index and type
      m_columnNameToIdxAndType->add(colName, idxAndType);
    }
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: " + ne.what());
  }
}

//------------------------------------------------------------------------------
// columnIsNull
//------------------------------------------------------------------------------
bool SqliteRset::columnIsNull(const char *const colName) const {
  const ColNameToIdxAndType::IdxAndType idxAndType = (*m_columnNameToIdxAndType)[colName];
  return SQLITE_NULL == idxAndType.colType;
}

//------------------------------------------------------------------------------
// columnText
//------------------------------------------------------------------------------
const char *SqliteRset::columnText(const char *const colName) const {
  const ColNameToIdxAndType::IdxAndType idxAndType = (*m_columnNameToIdxAndType)[colName];
  if(SQLITE_NULL == idxAndType.colType) {
    return "";
  } else {
    return (const char *) sqlite3_column_text(m_stmt.get(), idxAndType.colIdx);
  }
}

//------------------------------------------------------------------------------
// columnUint64
//------------------------------------------------------------------------------
uint64_t SqliteRset::columnUint64(const char *const colName) const {
  const ColNameToIdxAndType::IdxAndType idxAndType = (*m_columnNameToIdxAndType)[colName];
  return (uint64_t)sqlite3_column_int64(m_stmt.get(), idxAndType.colIdx);
}

} // namespace catalogue
} // namespace cta
