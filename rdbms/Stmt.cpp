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

#include "Stmt.hpp"

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Stmt::Stmt(const std::string &sql, const AutocommitMode autocommitMode):
  m_sql(sql),
  m_paramNameToIdx(sql),
  m_autoCommitMode(autocommitMode) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Stmt::~Stmt() throw() {
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &Stmt::getSql() const {
  return m_sql;
}

//------------------------------------------------------------------------------
// getSqlForException
//------------------------------------------------------------------------------
std::string Stmt::getSqlForException() const {
  if(m_sql.length() <= c_maxSqlLenInExceptions) {
    return m_sql;
  } else {
    if(c_maxSqlLenInExceptions >= 3) {
      return m_sql.substr(0, c_maxSqlLenInExceptions - 3) + "...";
    } else {
      return std::string("..."). substr(0, c_maxSqlLenInExceptions);
    }
  }
}

//------------------------------------------------------------------------------
// bindBool
//------------------------------------------------------------------------------
void Stmt::bindBool(const std::string &paramName, const bool paramValue) {
  bindOptionalBool(paramName, paramValue);
}

//------------------------------------------------------------------------------
// bindOptionalBool
//------------------------------------------------------------------------------
void Stmt::bindOptionalBool(const std::string &paramName, const optional<bool> &paramValue) {
  if(paramValue) {
    bindOptionalUint64(paramName, paramValue.value() ? 1 : 0);
  } else {
    bindOptionalUint64(paramName, nullopt);
  }
}

} // namespace rdbms
} // namespace cta
