/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  enum class Status {
    SUCCESS,
    FAILED
  };

  virtual ~SchemaCheckerResult() = default;

  static std::string statusToString(const Status& status);

  /**
   * We can combine the SchemaComparerResult in order to add other Results to it
   *
   * Note: The status will never change if either this or other is FAILED. It will simply append the
   *       list of differences of other to this SchemaComparerResult.
   *
   * @param other the SchemaComparerResult object to add
   * @return combined SchemaCheckerResult
   */
  SchemaCheckerResult operator+=(const SchemaCheckerResult& other);
  /**
   * Prints the errors the ostream
   * @param os the ostream to print the errors
   */
  void displayErrors(std::ostream& os) const;
  /**
   * Prints the warnings the ostream
   * @param os the ostream to print the warnings
   */
  void displayWarnings(std::ostream& os) const;
  /**
   * Returns the status of the SchemaComparerResult
   * @return the status of the SchemaComparerResult
   */
  Status getStatus() const { return m_status; }
  /**
   * Add an error to this result
   * @param error the error to add
   */
  void addError(const std::string& error);
  /**
   * Add a warning to this result
   * @param warning the warning to add
   */
  void addWarning(const std::string& warning);

private:
  std::list<std::string> m_errors;
  std::list<std::string> m_warnings;
  Status m_status = Status::SUCCESS;
};

} // namespace cta::catalogue
