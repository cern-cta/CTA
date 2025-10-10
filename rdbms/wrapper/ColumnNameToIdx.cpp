/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/ColumnNameToIdx.hpp"

namespace cta::rdbms::wrapper {

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

} // namespace cta::rdbms::wrapper
