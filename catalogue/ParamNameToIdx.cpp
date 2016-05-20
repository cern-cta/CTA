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

#include "catalogue/ParamNameToIdx.hpp"
#include "common/exception/Exception.hpp"

#include <sstream>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ParamNameToIdx::ParamNameToIdx(const std::string &sql) {
  bool waitingForAParam = true;
  std::ostringstream paramName;
  unsigned int paramIdx = 1;

  for(const char *ptr = sql.c_str(); ; ptr++) {
    if(waitingForAParam) {
      if('\0' == *ptr) {
        break;
      } else if(':' == *ptr) {
        waitingForAParam = false;
        paramName << ":";
      }
    } else {
      if(!isValidParamNameChar(*ptr)) {
        if(paramName.str().empty()) {
          throw exception::Exception("Parse error: Empty SQL parameter name");
        }
        if(m_nameToIdx.find(paramName.str()) != m_nameToIdx.end()) {
          throw exception::Exception("Parse error: SQL parameter " + paramName.str() + " is a duplicate");
        }
        m_nameToIdx[paramName.str()] = paramIdx;
        paramName.clear();
        paramIdx++;
        waitingForAParam = true;
      }

      if('\0' == *ptr) {
        break;
      }

      if(':' == *ptr) {
        throw exception::Exception("Parse error: Consecutive SQL parameter names are not permitted");
      } else {
        paramName << *ptr;
      }
    }
  }
}

//------------------------------------------------------------------------------
// isValidParamNameChar
//------------------------------------------------------------------------------
bool ParamNameToIdx::isValidParamNameChar(const char c) {
  return ('0' <= c && c <= '9') ||
         ('A' <= c && c <= 'Z') ||
         ('a' <= c && c <= 'z') ||
         c == '_';
}

//------------------------------------------------------------------------------
// getIdx
//------------------------------------------------------------------------------
unsigned int ParamNameToIdx::getIdx(const std::string &paramName) const {
  auto itor = m_nameToIdx.find(paramName);
  if(itor == m_nameToIdx.end()) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: The SQL parameter " + paramName +
      " does not exist");
  }
  return itor->second;
}

} // namespace catalogue
} // namespace cta
