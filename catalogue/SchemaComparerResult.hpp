/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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

#include <list>
#include <string>

namespace cta {
namespace catalogue {
  /**
   * This class holds the results of the Schema comparison against the catalogue database schema
   * It is simply composed of:
   * - a list of differences between the catalogue schema and the schema we are comparing it against
   * - a Status (SUCCESS or FAILED)
   */
class SchemaComparerResult {
public:
  /**
   * The comparison is either SUCCESS or FAILED
   */
  enum Status {
    SUCCESS,
    FAILED
  };
  static std::string StatusToString(const Status& status);
  
  SchemaComparerResult();
  SchemaComparerResult(const SchemaComparerResult& orig);
  SchemaComparerResult operator=(const SchemaComparerResult &other);
  /**
   * We can combine the SchemaComparerResult in order to add other Results to it
   * @param other the SchemaComparerResult object to add
   * @return other appended to this Schema ComparerResult
   * 
   * Note: The status will never change if it is failed (this or other)
   * It will simply append the list of differences of other to this SchemaComparerResult
   */
  SchemaComparerResult operator+=(const SchemaComparerResult &other);
  /**
   * Prints the differences in this result
   */
  void printDiffs() const;
  /**
   * Returns the status of the SchemaComparerResult
   * @return the status of the SchemaComparerResult
   */
  Status getStatus() const;
  /**
   * Add a difference to this Result
   * @param diff the difference to add
   */
  void addDiff(const std::string &diff);
  virtual ~SchemaComparerResult();
private:
  std::list<std::string> m_diffs;
  Status m_status;
};

}}
