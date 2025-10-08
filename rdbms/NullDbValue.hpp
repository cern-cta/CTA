/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"

namespace cta::rdbms {

/**
 * An exception class representing an unexpected encounter with a nullptr database
 * value (not to be confused with a nullptr C/C++ pointer).
 */
class NullDbValue: public exception::Exception {
public:

  /**
   * Constructor.
   *
   * @param context optional context string added to the message
   * at initialisation time.
   * @param embedBacktrace whether to embed a backtrace of where the
   * exception was throw in the message
   */
  NullDbValue(const std::string &context = "", const bool embedBacktrace = true);

}; // class NullDbValue

} // namespace cta::rdbms
