/******************************************************************************
 *                      Utils/Regex.hpp
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

#pragma once
#include <vector>
#include <string>
#include <regex.h>

/**
 * Semi-trivial wrapper to regex library, mostly 
 * useful for its destructor which will allow
 * RAII.
 */
namespace castor {
namespace tape {
namespace utils {

  class Regex {
  public:
    Regex(const char * re_str);
    virtual ~Regex();
    std::vector<std::string> exec(const std::string &s);
  private:
    regex_t m_re;
    bool m_set;
  }; /* class regex */
} // namespace Utils
} // namespace tape
} // namespace castor
