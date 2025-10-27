/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "common/log/LogContext.hpp"

namespace cta::maintenance {

class IMaintenanceRunner {
public:
  virtual ~IMaintenanceRunner() = default;

  /**
   * Execute the main logic of a runner.
   *
   * Different runners can freely implement the work logic and looping (if necessary).
   * The only compromise from this interface is that the total amount of work
   * carried out by an execution of the runner should be completed within a fixed
   * amount of time dictated by the contents of the cta-maintenance service file.
   * This allows for a graceful termination when SIGTERM is received.
   *
   * If a new timeout is put in place that is bigger than any other runner's
   * timeout, the service file should be updated with the new value.
   */
  virtual void executeRunner(cta::log::LogContext& lc) = 0;
};

}  // namespace cta::maintenance
