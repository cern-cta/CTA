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

#include <map>
#include <stdexcept>
#include <string>

namespace cta {
namespace catalogue {

/**
 * A map from column name to column index.
 *
 * Please note that the implementation of this class is intentionally in-lined
 * so it can be used by code compiled against the CXX11 ABI and by code compiled
 * against a newer ABI.
 */
class ColumnNameToIdx {
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
      throw std::runtime_error(std::string(__FUNCTION__) + " failed: Unknown volumn name " + name);
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

}; // class ColumnNameToIdx

} // namespace catalogue
} // namespace cta
