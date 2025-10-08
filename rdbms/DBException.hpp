/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"

#include <string>


namespace cta::rdbms {

/**
 * A database constraint error.
 */
class DBException : public cta::exception::Exception {
public:

  /**
   * Constructor.
   *
   * @param context optional context string added to the message
   * at initialisation time.
   * @param embedBacktrace whether to embed a backtrace of where the
   * exception was throw in the message
   */
  DBException(const std::string &context = "", const std::string &dbErrorMessage="", const bool embedBacktrace = true);

  /**
   * Empty Destructor, explicitely non-throwing (needed for std::exception
   * inheritance)
   */
  ~DBException() noexcept override = default;

  /**
   * Returns the raw error message generated bu the DB
   */
  std::string getDbErrorMessage() const;

private:
  std::string rawDbErrorMessage;
}; // class DBException

} // namespace cta::rdbms
