/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <map>
#include <string>

namespace cta::rdbms::wrapper {

/**
 * A map from column name to column index.
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
  void add(const std::string &name, const int idx);

  /**
   * Returns the index of the column with the specified name.
   *
   * This method throws an exception if the specified column name is not in the
   * map.
   *
   * @return the index of the column with the specified name.
   */
  int getIdx(const std::string &name) const;

  /**
   * Returns true if this map is empty.
   *
   * @return True if this map is empty.
   */
  bool empty() const;

private:

  /**
   * The underlying STL map from column name to column index.
   */
  std::map<std::string, int> m_nameToIdx;

}; // class ColumnNameToIdx

} // namespace cta::rdbms::wrapper
