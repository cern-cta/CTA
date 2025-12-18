/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * An exception class representing an application error that should not be retried
 */
class NonRetryableError : public exception::Exception {
public:
  /**
   * Constructor
   *
   * @param context optional context string added to the message at initialisation time
   * @param embedBacktrace whether to embed a backtrace of where the exception was thrown in the message
   */
  explicit NonRetryableError(const std::string& context = "", const bool embedBacktrace = true)
      : Exception(context, embedBacktrace) {}
};

}  // namespace cta::exception
