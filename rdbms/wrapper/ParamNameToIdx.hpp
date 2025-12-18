/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <map>
#include <stdint.h>
#include <string>

namespace cta::rdbms::wrapper {

/**
 * Map from SQL parameter name to parameter index
 */
class ParamNameToIdx {
public:
  /**
   * Constructor
   *
   * Parses the specified SQL statement to populate an internal map from SQL
   * parameter name to parameter index.
   *
   * @param sql The SQL statement to be parsed for SQL parameter names
   */
  explicit ParamNameToIdx(const std::string& sql);

  /**
   * Returns the index of the specified SQL parameter
   *
   * @param paramName The name of the SQL parameter
   * @return The index of the SQL parameter
   */
  uint32_t getIdx(const std::string& paramName) const;

private:
  /**
   * Map from SQL parameter name to parameter index.
   */
  std::map<std::string, uint32_t> m_nameToIdx;
};

}  // namespace cta::rdbms::wrapper
