/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/ConstraintError.hpp"

namespace cta::rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ConstraintError::ConstraintError(const std::string &context, const std::string &dbErrorMessage, const std::string &violatedConstraintName, const bool embedBacktrace):
  DBException{context, dbErrorMessage, embedBacktrace}, m_violatedConstraintName{violatedConstraintName} {
}

//------------------------------------------------------------------------------
// getDbErrorMessage
//------------------------------------------------------------------------------
std::string ConstraintError::getViolatedConstraintName() const {
  return m_violatedConstraintName;
}

} // namespace cta::rdbms
