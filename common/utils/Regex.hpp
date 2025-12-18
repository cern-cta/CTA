/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once
#include <regex.h>
#include <string>
#include <vector>

/*!
 * Semi-trivial wrapper to regex library, mostly useful for its destructor which will allow RAII
 */
namespace cta::utils {

class Regex {
public:
  explicit Regex(const std::string& re_str);
  Regex(const Regex& o);
  virtual ~Regex();

  /*!
     * Return a list of matching substrings
     */
  std::vector<std::string> exec(const std::string& s) const;

  /*!
     * Return true if there is at least one matching substring
     */
  bool has_match(const std::string& s) const { return ::regexec(&m_re, s.c_str(), 0, nullptr, 0) != REG_NOMATCH; }

private:
  // We keep around the string from which the RE was compiled to allow copying.
  std::string m_reStr;
  regex_t m_re;
  bool m_set;
};

}  // namespace cta::utils
