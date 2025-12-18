/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/ColumnNameToIdxAndType.hpp"

#include "common/exception/Exception.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// add
//------------------------------------------------------------------------------
void ColumnNameToIdxAndType::add(const std::string& name, const IdxAndType& idxAndType) {
  if (m_nameToIdxAndType.end() != m_nameToIdxAndType.find(name)) {
    throw exception::Exception("Duplicate column name: " + name);
  }
  m_nameToIdxAndType[name] = idxAndType;
}

//------------------------------------------------------------------------------
// getIdxAndType
//------------------------------------------------------------------------------
ColumnNameToIdxAndType::IdxAndType ColumnNameToIdxAndType::getIdxAndType(const std::string& name) const {
  auto it = m_nameToIdxAndType.find(name);
  if (m_nameToIdxAndType.end() == it) {
    throw exception::Exception("Unknown column name: " + name);
  }
  return it->second;
}

//------------------------------------------------------------------------------
// empty
//------------------------------------------------------------------------------
bool ColumnNameToIdxAndType::empty() const {
  return m_nameToIdxAndType.empty();
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void ColumnNameToIdxAndType::clear() {
  m_nameToIdxAndType.clear();
}

}  // namespace cta::rdbms::wrapper
