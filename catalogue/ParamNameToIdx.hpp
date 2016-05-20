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

#include <map>
#include <string>

namespace cta {
namespace catalogue {

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
  unsigned int getIdx(const std::string &paramName) const;

private:

  /**
   * Map from SQL parameter name to parameter index.
   */
  std::map<std::string, unsigned int> m_nameToIdx;

  /**
   * Returns true if the specified character is a valid parameter name
   * character.
   *
   * @param c The character.
   * @return True if the specified character is a valid parameter name
   * character.
   */
  bool isValidParamNameChar(const char c);

}; // class ParamNameToIdx

} // namespace catalogue
} // namespace cta
