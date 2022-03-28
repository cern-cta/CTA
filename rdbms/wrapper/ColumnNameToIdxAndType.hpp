/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <map>
#include <string>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * A map from column name to column index and type.
 */
class ColumnNameToIdxAndType {
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
  void add(const std::string &name, const IdxAndType &idxAndType);

  /**
   * Returns the index and type of the column with the specified name.
   *
   * This method throws an exception if the specified column name is not in the
   * map.
   *
   * @param name The name of the column.
   * @return The index and type of the column.
   */
  IdxAndType getIdxAndType(const std::string &name) const;

  /**
   * Returns true if this map is empty.
   *
   * @return True if this map is empty.
   */
  bool empty() const;

  /**
   * Clears the contents of this map.
   */
  void clear();

private:

  /**
   * The underlying STL map from column name to column index.
   */
  std::map<std::string, IdxAndType> m_nameToIdxAndType;

}; // class ColumnNameToIdxAndType

} // namespace wrapper
} // namespace rdbms
} // namespace cta
