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
#include <stdint.h>
#include <string>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * Map from SQL parameter name to parameter index.
 */
class ParamNameToIdx {
public:
  /**
   * Constructor.
   *
   * Parses the specified SQL statement to populate an internal map from SQL
   * parameter name to parameter index.
   *
   * @param sql The SQL statement to be parsed for SQL parameter names.
   */
  ParamNameToIdx(const std::string &sql);

  /**
   * Returns the index of teh specified SQL parameter.
   *
   * @param paramNAme The name of the SQL parameter.
   * @return The index of the SQL parameter.
   */
  uint32_t getIdx(const std::string &paramName) const;

  /**
   * Returns true if the specified character is a valid parameter name
   * character.
   *
   * @param c The character.
   * @return True if the specified character is a valid parameter name
   * character.
   */
  static bool isValidParamNameChar(const char c);

private:

  /**
   * Map from SQL parameter name to parameter index.
   */
  std::map<std::string, uint32_t> m_nameToIdx;

}; // class ParamNameToIdx

} // namespace wrapper
} // namespace rdbms
} // namespace cta
