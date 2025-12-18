/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "NullDbValue.hpp"

namespace cta::rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
NullDbValue::NullDbValue(const std::string& context, const bool embedBacktrace) : Exception(context, embedBacktrace) {}

}  // namespace cta::rdbms
