/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/UserError.hpp"

namespace cta::exception {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
UserError::UserError(const std::string &context, const bool embedBacktrace):
  Exception(context, embedBacktrace) {
}

} // namespace cta::exception
