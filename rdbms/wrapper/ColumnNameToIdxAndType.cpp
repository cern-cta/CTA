/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/ColumnNameToIdxAndType.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// add
//------------------------------------------------------------------------------
void ColumnNameToIdxAndType::add(const std::string &name, const IdxAndType &idxAndType) {
  if(m_nameToIdxAndType.end() != m_nameToIdxAndType.find(name)) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + name + " is a duplicate");
  }
  m_nameToIdxAndType[name] = idxAndType;
}

//------------------------------------------------------------------------------
// getIdxAndType
//------------------------------------------------------------------------------
ColumnNameToIdxAndType::IdxAndType ColumnNameToIdxAndType::getIdxAndType(const std::string &name) const {
  auto it = m_nameToIdxAndType.find(name);
  if(m_nameToIdxAndType.end() == it) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Unknown column name " + name);
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

} // namespace wrapper
} // namespace rdbms
} // namespace cta
