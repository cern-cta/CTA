/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "Regex.hpp"
#include "common/exception/Exception.hpp"
#include <regex.h>

namespace cta { namespace utils {

Regex::Regex(const std::string & re_str) : m_reStr(re_str), m_set(false) {
  if (int rc = ::regcomp(&m_re, m_reStr.c_str(), REG_EXTENDED)) {
    std::string error("Could not compile regular expression: \"");
    error += m_reStr;
    error += "\"";
    char re_err[1024];
    if (::regerror(rc, &m_re, re_err, sizeof (re_err))) {
      error += ": ";
      error += re_err;
    }
    throw cta::exception::Exception(error);
  }
  m_set = true;
}

Regex::Regex(const Regex& o) {
  Regex(o.m_reStr);
}

Regex::~Regex() {
  if (m_set)
    ::regfree(&m_re);
}

std::vector<std::string> Regex::exec(const std::string &s) const {
  regmatch_t matches[100];
  if (REG_NOMATCH != ::regexec(&m_re, s.c_str(), 100, matches, 0)) {
    std::vector<std::string> ret;
    for (int i = 0; i < 100; i++) {
      if (matches[i].rm_so != -1) {
        ret.push_back(s.substr(matches[i].rm_so, matches[i].rm_eo - matches[i].rm_so));
      } else
        break;
    }
    return ret;
  }
  return std::vector<std::string>();
}

}} // namespace cta::utils
