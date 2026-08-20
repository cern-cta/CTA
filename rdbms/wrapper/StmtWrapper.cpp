/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/StmtWrapper.hpp"

#include "rdbms/rdbms.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StmtWrapper::StmtWrapper(const std::string& sql) : m_sql(sql), m_paramNameToIdx(sql) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
StmtWrapper::~StmtWrapper() = default;

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string& StmtWrapper::getSql() const {
  return m_sql;
}

//------------------------------------------------------------------------------
// setDbQuerySummary
//------------------------------------------------------------------------------
void StmtWrapper::setDbQuerySummary(const std::string& optQuerySummary) {
  m_queryType = optQuerySummary;
}

//------------------------------------------------------------------------------
// getDbQuerySummary
//------------------------------------------------------------------------------
const std::string StmtWrapper::getDbQuerySummary() const {
  return m_queryType;
}

//------------------------------------------------------------------------------
// getParamIdx
//------------------------------------------------------------------------------
uint32_t StmtWrapper::getParamIdx(const std::string& paramName) const {
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
void StmtWrapper::bindBool(const std::string& paramName, const std::optional<bool>& paramValue) {
  if (paramValue.has_value()) {
    bindString(paramName, paramValue.value() ? std::string("1") : std::string("0"));
  } else {
    bindString(paramName, std::nullopt);
  }
}

}  // namespace cta::rdbms::wrapper
