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

//------------------------------------------------------------------------------
// addNameToIdx
//------------------------------------------------------------------------------
void cta::catalogue::ColumnNameToIdx::add(const std::string &name,
  const int idx) {
  if(m_nameToIdx.end() != m_nameToIdx.find(name)) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: Column name " << name <<
      " is a duplicate";
    throw ex;
  }
  m_nameToIdx[name] = idx;
}

//------------------------------------------------------------------------------
// getIdx
//------------------------------------------------------------------------------
int cta::catalogue::ColumnNameToIdx::getIdx(const std::string &name) const {
  auto it = m_nameToIdx.find(name);
  if(m_nameToIdx.end() == it) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: Unknown column name " << name;
    throw ex;
  }
  return it->second;
}

//------------------------------------------------------------------------------
// operator[]
//------------------------------------------------------------------------------
int cta::catalogue::ColumnNameToIdx::operator[](const std::string &name) const {
  return getIdx(name);
}

//------------------------------------------------------------------------------
// empty
//------------------------------------------------------------------------------
bool cta::catalogue::ColumnNameToIdx::empty() const {
  return m_nameToIdx.empty();
}
