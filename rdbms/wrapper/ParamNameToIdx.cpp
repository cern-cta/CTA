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

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/ParamNameToIdx.hpp"

#include <regex>
#include <sstream>

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ParamNameToIdx::ParamNameToIdx(const std::string &sql) {
  std::ostringstream paramName;
  uint32_t paramIdx = 0;
  // Match any :name that is not preceded by another colon
  std::regex pattern(R"(:(\w+))");
  std::smatch match;
  std::string::const_iterator searchStart(sql.cbegin());
  while (std::regex_search(searchStart, sql.cend(), match, pattern)) {
    auto matchPos = std::distance(sql.cbegin(), searchStart) + match.position();
    if (matchPos > 0 && ':' == *(sql.cbegin() + matchPos -1)){
      searchStart = match.suffix().first;
      continue;
    }
    paramName << match.str();
    if(paramName.str().empty()) {
      throw exception::Exception("Parse error: Empty SQL parameter name");
    }
    if(m_nameToIdx.find(paramName.str()) != m_nameToIdx.end()) {
      throw exception::Exception("Parse error: SQL parameter " + paramName.str() + " is a duplicate");
    }
    ++paramIdx; // Increment the parameter counter
    m_nameToIdx[paramName.str()] = paramIdx;
    paramName.str(std::string()); // Clear the stream
    searchStart = match.suffix().first;
  }
}

//------------------------------------------------------------------------------
// getIdx
//------------------------------------------------------------------------------
uint32_t ParamNameToIdx::getIdx(const std::string &paramName) const {
  auto itor = m_nameToIdx.find(paramName);
  if(itor == m_nameToIdx.end()) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: The SQL parameter " + paramName +
      " does not exist");
  }
  return itor->second;
}

} // namespace cta::rdbms::wrapper
