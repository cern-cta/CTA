/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/DBException.hpp"

#include <string>


namespace cta::rdbms {

/**
 * A database constraint error.
 */
class ConstraintError : public DBException {
public:
  /**
   * Constructor
   *
   * @param context optional context string added to the message
   * at initialisation time.
   * @param embedBacktrace whether to embed a backtrace of where the
   * exception was throw in the message
   */
  ConstraintError(const std::string &context, const std::string &dbErrorMessage, const std::string &violatedConstraintName, const bool embedBacktrace = true);

  /**
   * Empty Destructor (needed for std::exception inheritance)
   */
  ~ConstraintError() override = default;

  /**
   * Returns the violated constraint name, as returned by the DB
   */
  std::string getViolatedConstraintName() const;

private:
  std::string m_violatedConstraintName;
}; // class ConstraintError

} // namespace cta::rdbms
