/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdApp.hpp"

#include "RoutineRunnerFactory.hpp"

namespace cta::maintd {

void MaintdApp::stop() {
  if (m_routineRunner) {
    m_routineRunner->stop();
  }
}

int MaintdApp::run(const MaintdConfig& config, cta::log::Logger& log) {
  cta::log::LogContext lc(log);
  // one connection per pool. no objectstore thread pool is needed
  service_resources::ConnectionOptions connections {1, 1};
  m_resources = std::make_unique<service_resources::ServiceResources>("Maintd",
                                                                      config.catalogue,
                                                                      config.scheduler,
                                                                      connections,
                                                                      log);
  maintd::RoutineRunnerFactory rrFactory(config, *m_resources, lc);
  m_routineRunner = rrFactory.create();
  m_routineRunner->run(lc);
  return 0;
}

bool MaintdApp::isReady() const {
  return m_routineRunner && m_routineRunner->isReady();
}

bool MaintdApp::isLive() const {
  if (!m_routineRunner) {
    // We consider ourselves alive if we haven't started yet, because a restart likely won't fix this.
    return true;
  }
  return m_routineRunner->isLive();
}

}  // namespace cta::maintd
