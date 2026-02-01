/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdApp.hpp"

#include "RoutineRunnerFactory.hpp"

namespace cta::maintd {

//------------------------------------------------------------------------------
// MaintdApp::stop
//------------------------------------------------------------------------------
void MaintdApp::stop() {
  if (m_routineRunner) {
    m_routineRunner->stop();
  }
}

//------------------------------------------------------------------------------
// MaintdApp::run
//------------------------------------------------------------------------------
int MaintdApp::run(const MaintdConfig& config, cta::log::Logger& log) {
  cta::log::LogContext lc(log);
  maintd::RoutineRunnerFactory rrFactory(config, lc);
  m_routineRunner = rrFactory.create();
  m_routineRunner->run(lc);
  return 0;
}

bool MaintdApp::isReady() const {
  return m_routineRunner && m_routineRunner->isReady();
}

/**
 * The routine runner is considered alive when:
 * - a routine has executed in the last 2 minutes
 */
bool MaintdApp::isLive() const {
  if (!m_routineRunner) {
    // We consider ourselves alive if we haven't started yet, because a restart likely won't fix this.
    return true;
  }
  return m_routineRunner->isLive();
}

}  // namespace cta::maintd
