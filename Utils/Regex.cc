// ----------------------------------------------------------------------
// File: Utils/Regex.cc
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "Regex.hh"
#include "../Exception/Exception.hh"
#include <regex.h>

Tape::Utils::regex::regex(const char * re_str) : m_set(false) {
  if (int rc = ::regcomp(&m_re, re_str, REG_EXTENDED)) {
    std::string error("Could not compile regular expression: \"");
    error += re_str;
    error += "\"";
    char re_err[1024];
    if (::regerror(rc, &m_re, re_err, sizeof (re_err))) {
      error += ": ";
      error += re_err;
    }
    throw Tape::Exception(error);
  }
  m_set = true;
}

Tape::Utils::regex::~regex() {
  if (m_set)
    ::regfree(&m_re);
}

std::vector<std::string> Tape::Utils::regex::exec(const std::string &s) {
  regmatch_t matches[100];
  if (REG_NOMATCH != ::regexec(&m_re, s.c_str(), 100, matches, 0)) {
    std::vector<std::string> ret;
    for (int i = 0; i < 100; i++) {
      if (matches[i].rm_so != -1) {
        ret.push_back(s.substr(matches[i].rm_so, matches[i].rm_eo - matches[i].rm_so + 1));
      } else
        break;
    }
    return ret;
  }
  return std::vector<std::string>();
}
