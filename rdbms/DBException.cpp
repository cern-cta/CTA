/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/DBException.hpp"

namespace cta::rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DBException::DBException(const std::string &context, const std::string &dbErrorMessage, const bool embedBacktrace):
  Exception(context, embedBacktrace), rawDbErrorMessage{dbErrorMessage} {
}

//------------------------------------------------------------------------------
// getDbErrorMessage
//------------------------------------------------------------------------------
std::string DBException::getDbErrorMessage() const {
  return rawDbErrorMessage;
}

} // namespace cta::rdbms
