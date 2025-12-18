/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"

namespace cta::maintd {

class IRoutine {
public:
  virtual ~IRoutine() = default;

  virtual std::string getName() const = 0;

  /**
   * Execute the main logic of a routine.
   *
   * Different routines can freely implement the work logic and looping (if necessary).
   * The only compromise from this interface is that the total amount of work
   * carried out by an execution of the routine should be completed within a fixed
   * amount of time dictated by the contents of the cta-maintd service file.
   * This allows for a graceful termination when SIGTERM is received.
   *
   * If a new timeout is put in place that is bigger than any other routine's
   * timeout, the service file should be updated with the new value.
   */
  virtual void execute() = 0;
};

}  // namespace cta::maintd
