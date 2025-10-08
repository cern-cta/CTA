/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/UniqueConstraintError.hpp"

namespace cta::rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
UniqueConstraintError::UniqueConstraintError(
        const std::string &context,
        const std::string &dbErrorMessage,
        const std::string &violatedConstraintName,
        const bool embedBacktrace):
  ConstraintError(context, dbErrorMessage, violatedConstraintName, embedBacktrace) {
}

} // namespace cta::rdbms
