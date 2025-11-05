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

#include "IRoutine.hpp"
#include "common/exception/Exception.hpp"
#include "common/process/SignalHandler.hpp"
#include "common/log/LogContext.hpp"

namespace cta::maintenance {

CTA_GENERATE_EXCEPTION_CLASS(InvalidConfiguration);

/**
 * Responsible for periodically executing the various routines.
 */
class RoutineRunner {
public:
  RoutineRunner(uint32_t sleepInterval);

  ~RoutineRunner() = default;

  void registerRoutine(std::unique_ptr<IRoutine> routine);

  /**
   * Periodically executes all registered routines.
   * After all routines have been executed, it will check if it has received any signals. If so, it will exit the run method.
   */
  uint32_t run(cta::log::LogContext& lc);

private:
  std::list<std::unique_ptr<IRoutine>> m_routines;

  std::unique_ptr<cta::SignalHandler> m_signalHandler = std::make_unique<cta::SignalHandler>();

  uint32_t m_sleepInterval;
};

}  // namespace cta::maintenance
