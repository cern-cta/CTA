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

#include "catalogue/ColumnNameToIdx.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// add
//------------------------------------------------------------------------------
void ColumnNameToIdx::add(const std::string &name, const int idx) {
  if(m_nameToIdx.end() != m_nameToIdx.find(name)) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + name + " is a duplicate");
  }
  m_nameToIdx[name] = idx;
}

//------------------------------------------------------------------------------
// getIdx
//------------------------------------------------------------------------------
int ColumnNameToIdx::getIdx(const std::string &name) const {
  auto it = m_nameToIdx.find(name);
  if(m_nameToIdx.end() == it) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Unknown column name " + name);
  }
  return it->second;
}

//------------------------------------------------------------------------------
// empty
//------------------------------------------------------------------------------
bool ColumnNameToIdx::empty() const {
  return m_nameToIdx.empty();
}

} // namespace catalogue
} // namespace cta
