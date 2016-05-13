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

// Version 12.1 of oracle instant client uses the pre _GLIBCXX_USE_CXX11_ABI
#define _GLIBCXX_USE_CXX11_ABI 0

#include "catalogue/OcciRset.hpp"
#include "catalogue/OcciStmt.hpp"

#include <cstring>
#include <map>
#include <stdexcept>

namespace cta {
namespace catalogue {

/**
 * A map from column name to column index.
 *
 * Please note that this class is intentionally hidden within this cpp file to
 * enable the OcciRset class to be used by code compiled against the CXX11 ABI
 * and by code compiled against a pre-CXX11 ABI.
 */
class OcciRset::ColumnNameToIdx {
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

}; // class OcciRset::ColumnNameToIdx

/**
 * Helper class that enables OcciRset to provide a C-string memory management
 * behaviour similar to that of the SQLite API.  A C-string returned by the
 * OcciRset::columnText() method will be automatically deleted either when
 * OcciRset::next() is called or when the OcciRset destructor is called.
 */
class OcciRset::TextColumnCache {
public:

  /**
   * Destructor.
   */
  ~TextColumnCache() {
    clear();
  }

  /**
   * Clears the cache, calling delete[] on any cached column values.
   */
  void clear() {
    for(auto nameAndValue : m_colNameToValue) {
      delete[] nameAndValue.second;
    }
    m_colNameToValue.clear();
  }

  /**
   * Sets the cached value of the specified column by copying the contents of
   * the specified string into the cache.
   *
   * This method will throw an exception if the specified column already has a
   * cached value.
   *
   * @param name The name of the column.
   * @param value The string value to be copied into the cache.
   * @return The duplicate value now in the cache.
   */
  const char *set(const char *name, const char *const value) {
    if(NULL == name) {
      throw std::runtime_error(std::string(__FUNCTION__) + " failed: name is NULL");
    }
    if(NULL == value) {
      throw std::runtime_error(std::string(__FUNCTION__) + " failed: value is NULL");
    }
    auto itor = m_colNameToValue.find(name);

    // If the value has not already been cached
    if(itor == m_colNameToValue.end()) {
      // Copy the value into the cache
      const std::size_t len = std::strlen(value);
      char *const cachedValue = new char[len + 1];
      std::memcpy(cachedValue, value, len);
      cachedValue[len] = '\0';
      m_colNameToValue[name] = cachedValue;
      return cachedValue;
    } else {
      throw std::runtime_error(std::string(__FUNCTION__) + " failed: Cannot set the cached value of the " +
        name + " column to " + value + " because the column already has the cached value of " + itor->second);
    }
  }

  /**
   * Returns the cached value for the specified column or NULL if there is no
   * cached value.
   *
   * @param columnName The name of teh column.
   * @return The cached value for the specified column or NULL if there is no
   * cached value.
   */
  const char *get(const char *const columnName) {
    auto itor = m_colNameToValue.find(columnName);

    // If the value has been found
    if(itor != m_colNameToValue.end()) {
      return itor->second;
    } else {
      return NULL;
    }
  }

private:

  /**
   * Map from column name to column value.
   */
  std::map<std::string, const char *> m_colNameToValue;
};

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciRset::OcciRset(OcciStmt &stmt, oracle::occi::ResultSet *const rset):
  m_stmt(stmt),
  m_rset(rset) {
  try {
    if (NULL == rset) {
      throw std::runtime_error("rset is NULL");
    }
    m_colNameToIdx.reset(new ColumnNameToIdx());
    populateColNameToIdxMap();
    m_textColumnCache.reset(new TextColumnCache());
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      ne.what());
  }
}

//------------------------------------------------------------------------------
// populateColNameToIdx
//------------------------------------------------------------------------------
void OcciRset::populateColNameToIdxMap() {
  using namespace oracle;

  try {
    const std::vector<occi::MetaData> columns = m_rset->getColumnListMetaData();
    for (unsigned int i = 0; i < columns.size(); i++) {
      // Column indices start at 1
      const unsigned int colIdx = i + 1;
      m_colNameToIdx->add(columns[i].getString(occi::MetaData::ATTR_NAME), colIdx);
    }
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: " + ne.what());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciRset::~OcciRset() throw() {
  try {
    close(); // Idempotent close()
  } catch(...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void OcciRset::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(NULL != m_rset) {
    m_stmt->closeResultSet(m_rset);
    m_rset = NULL;
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::ResultSet *OcciRset::get() const {
  return m_rset;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::ResultSet *OcciRset::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool OcciRset::next() {
  using namespace oracle;

  try {
    m_textColumnCache->clear();
    const occi::ResultSet::Status status = m_rset->next();
    return occi::ResultSet::DATA_AVAILABLE == status;
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      ne.what());
  }
}

//------------------------------------------------------------------------------
// columnText
//------------------------------------------------------------------------------
const char * OcciRset::columnText(const char *const colName) {
  try {
    std::lock_guard<std::mutex> lock(m_mutex);

    const int colIdx = m_colNameToIdx->getIdx(colName);

    if(m_rset->isNull(colIdx)) {
      return NULL;
    } else {
      const char *cachedText = m_textColumnCache->get(colName);

      if(NULL != cachedText) {
        return cachedText;
      } else {
        return m_textColumnCache->set(colName, m_rset->getString(colIdx).c_str());
      }
    }
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      ne.what());
  }
}

} // namespace catalogue
} // namespace cta
