/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/GrpcError.hpp"


// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
cta::exception::GrpcError::GrpcError(const std::string &context, const bool embedBacktrace):
  cta::exception::Exception(context, embedBacktrace) {
}
