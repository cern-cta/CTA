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

#pragma once

#include <list>
#include <string>

namespace cta::catalogue {

  /**
   * This class holds the results of the Schema comparison against the catalogue database schema
   * It is simply composed of:
   * - a list of differences between the catalogue schema and the schema we are comparing it against
   * - a list of warnings
   * - a Status (SUCCESS or FAILED)
   */
class SchemaCheckerResult {
public:
  /**
   * The comparison is either SUCCESS or FAILED
   */
  enum Status {
    SUCCESS,
    FAILED
  };
  static std::string statusToString(const Status& status);
  
  SchemaCheckerResult();
  SchemaCheckerResult(const SchemaCheckerResult& orig);
  SchemaCheckerResult& operator=(const SchemaCheckerResult& other);
  /**
   * We can combine the SchemaComparerResult in order to add other Results to it
   * @param other the SchemaComparerResult object to add
   * @return other appended to this Schema ComparerResult
   * 
   * Note: The status will never change if it is failed (this or other)
   * It will simply append the list of differences of other to this SchemaComparerResult
   */
  SchemaCheckerResult operator+=(const SchemaCheckerResult &other);
  
  /**
   * Prints the errors the ostream
   * @param os the ostream to print the errors
   */
  void displayErrors(std::ostream & os) const;
  
  /**
   * Prints the warnings the ostream
   * @param os the ostream to print the warnings
   */
  void displayWarnings(std::ostream & os) const;
  /**
   * Returns the status of the SchemaComparerResult
   * @return the status of the SchemaComparerResult
   */
  Status getStatus() const;
  /**
   * Add an error to this result
   * @param error the error to add
   */
  void addError(const std::string &error);
  /**
   * Add a warning to this result
   * @param warning the warning to add
   */
  void addWarning(const std::string & warning);
  
  virtual ~SchemaCheckerResult() = default;
private:
  std::list<std::string> m_errors;
  std::list<std::string> m_warnings;
  Status m_status;
};

} // namespace cta::catalogue
