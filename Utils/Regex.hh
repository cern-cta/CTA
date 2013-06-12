// ----------------------------------------------------------------------
// File: Utils/Regex.hh
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

#pragma once
#include <vector>
#include <string>
#include <regex.h>

/**
 * Semi-trivial wrapper to regex library, mostly 
 * useful for its destructor which will allow
 * RAII.
 */
namespace Tape {
  namespace Utils {

    class regex {
    public:
      regex(const char * re_str);
      virtual ~regex();
      std::vector<std::string> exec(const std::string &s);
    private:
      regex_t m_re;
      bool m_set;
    }; /* class regex */
  }; /* namespace Utils */
}; /* namespace Tape */
