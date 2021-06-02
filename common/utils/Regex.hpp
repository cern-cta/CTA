/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <vector>
#include <string>
#include <regex.h>

/*!
 * Semi-trivial wrapper to regex library, mostly useful for its destructor which will allow RAII
 */
namespace cta { namespace utils {

class Regex {
  public:
    Regex(const std::string & re_str);
    Regex(const Regex & o);
    virtual ~Regex();

    /*!
     * Return a list of matching substrings
     */
    std::vector<std::string> exec(const std::string &s) const;

    /*!
     * Return true if there is at least one matching substring
     */
    bool has_match(const std::string &s) const {
      return ::regexec(&m_re, s.c_str(), 0, nullptr, 0) != REG_NOMATCH;
    }

  private:
    // We keep around the string from which the RE was compiled to allow copying.
    std::string m_reStr;
    regex_t m_re;
    bool m_set;
};

}} // namespace cta::utils
