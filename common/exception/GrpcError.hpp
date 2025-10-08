/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

    class GrpcError : public cta::exception::Exception {

    public:

      /**
       * Constructor
       */
      GrpcError(const std::string &context, const bool embedBacktrace = true);

    }; // class GrpcError

} // namespace cta exception
