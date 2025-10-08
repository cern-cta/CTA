/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "rdbms/ConstraintError.hpp"

#include <string>


namespace cta::rdbms {

/**
 * A database constraint has been violated.
 */
class UniqueConstraintError : public ConstraintError {
public:

  /**
   * Constructor.
   *
   * @param context optional context string added to the message
   * at initialisation time.
   * @param embedBacktrace whether to embed a backtrace of where the
   * exception was throw in the message
   */
  UniqueConstraintError(const std::string &context, const std::string &dbErrorMessage, const std::string &violatedConstraintName, const bool embedBacktrace = true);

  /**
   * Empty Destructor, explicitely non-throwing (needed for std::exception
   * inheritance)
   */
  ~UniqueConstraintError() noexcept override = default;

}; // class UniqueConstraintError

} // namespace cta::rdbms
