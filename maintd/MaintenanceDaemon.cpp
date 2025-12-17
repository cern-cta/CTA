/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintenanceDaemon.hpp"

#include "RoutineRunnerFactory.hpp"

#include <map>

namespace cta::maintd {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MaintenanceDaemon::MaintenanceDaemon(cta::common::Config& config, cta::log::LogContext& lc)
    : m_config(config),
      m_lc(lc) {}

void MaintenanceDaemon::stop() {
  if (!m_routineRunner) {
    m_lc.log(log::WARNING, "In MaintenanceDaemon::stop(): Stop requested but daemon is not currently active");
    return;
  }
  m_stopRequested = true;
  m_routineRunner->stop();
}

void MaintenanceDaemon::run() {
  m_stopRequested = false;
  while (!m_stopRequested) {
    // Important: the RoutineRunnerFactory is the owner of resources such as the scheduler and catalogue.
    // The routines are implemented in such a way that they take references to the scheduler and catalogue.
    // As such, the routines themselves don't own these resources (which is not ideal).
    // Instead, the RoutineRunnerFactory is the owner of these resources and must live as long as the routines themselves.
    // There does not seem to be a cleaner way to do this without making various changes to the objectstore implementation.
    // Ideally each routine owns its own resources, but that means multiplying the number of agents by the number of routines.

    RoutineRunnerFactory routineRunnerFactory(m_config, m_lc);
    // Create the routine runner
    m_routineRunner = routineRunnerFactory.create();
    // This run routine blocks until we explicitly tell the routine runner to stop
    m_routineRunner->run(m_lc);
  }
  m_lc.log(cta::log::INFO, "In MaintenanceDaemon::run: Maintenance daemon stopped");
}

void MaintenanceDaemon::reload() {
  if (!m_routineRunner) {
    m_lc.log(log::WARNING, "In MaintenanceDaemon::reload(): Reload requested but daemon is not currently active");
    return;
  }

  m_lc.log(cta::log::INFO,
           "Reloading config for maintd process. Process user and group, telemetry, and log format will not be "
           "reloaded.");

  cta::log::Logger& logger = m_lc.logger();
  // Reload the config
  m_config.parse(logger);

  // Update log mask
  logger.setLogMask(m_config.getOptionValueStr("cta.log.level").value_or("INFO"));

  // Stopping the routineRunner will restart the loop in run() and therefore recreate the routines with the new config
  m_routineRunner->stop();
}

}  // namespace cta::maintd
