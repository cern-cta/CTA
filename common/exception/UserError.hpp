/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * An exception class representng a user as opposed to an application error.
 */
class UserError: public exception::Exception {
public:

  /**
   * Constructor.
   *
   * @param context optional context string added to the message
   * at initialisation time.
   * @param embedBacktrace whether to embed a backtrace of where the
   * exception was throw in the message
   */
  UserError(const std::string &context = "", const bool embedBacktrace = true);

}; // class UserError

} // namespace cta::exception

#define CTA_GENERATE_USER_EXCEPTION_CLASS(A) class A: public cta::exception::UserError { using UserError::UserError; }
