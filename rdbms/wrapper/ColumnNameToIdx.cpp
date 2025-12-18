/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/ColumnNameToIdx.hpp"

#include "common/exception/Exception.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// add
//------------------------------------------------------------------------------
void ColumnNameToIdx::add(const std::string& name, const int idx) {
  if (m_nameToIdx.end() != m_nameToIdx.find(name)) {
    throw exception::Exception("Duplicate column name: " + name);
  }
  m_nameToIdx[name] = idx;
}

//------------------------------------------------------------------------------
// getIdx
//------------------------------------------------------------------------------
int ColumnNameToIdx::getIdx(const std::string& name) const {
  auto it = m_nameToIdx.find(name);
  if (m_nameToIdx.end() == it) {
    throw exception::Exception("Unknown column name: " + name);
  }
  return it->second;
}

//------------------------------------------------------------------------------
// empty
//------------------------------------------------------------------------------
bool ColumnNameToIdx::empty() const {
  return m_nameToIdx.empty();
}

}  // namespace cta::rdbms::wrapper
