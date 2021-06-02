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

#include "rdbms/rdbms.hpp"
#include "rdbms/wrapper/StmtWrapper.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StmtWrapper::StmtWrapper(const std::string &sql):
  m_sql(sql),
  m_paramNameToIdx(sql) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
StmtWrapper::~StmtWrapper() {
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &StmtWrapper::getSql() const {
  return m_sql;
}

//------------------------------------------------------------------------------
// getParamIdx
//------------------------------------------------------------------------------
uint32_t StmtWrapper::getParamIdx(const std::string &paramName) const {
  return m_paramNameToIdx.getIdx(paramName);
}

//------------------------------------------------------------------------------
// getSqlForException
//------------------------------------------------------------------------------
std::string StmtWrapper::getSqlForException(const std::string::size_type maxSqlLenInExceptions) const {
  return rdbms::getSqlForException(m_sql, maxSqlLenInExceptions);
}

//------------------------------------------------------------------------------
// bindBool
//------------------------------------------------------------------------------
void StmtWrapper::bindBool(const std::string &paramName, const optional<bool> &paramValue) {
  if(paramValue) {
    bindString(paramName, paramValue.value() ? "1" : "0");
  } else {
    bindString(paramName, nullopt);
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
