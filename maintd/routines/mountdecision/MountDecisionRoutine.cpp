/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountDecisionRoutine.hpp"

#include "common/utils/utils.hpp"

#include <cstdlib>

namespace cta::maintd {

namespace {

std::string getMountDecisionCounterKey() {
  const auto hostName = cta::utils::getShortHostname();
  const char* const myName = std::getenv("MY_NAME");
  if (myName != nullptr && myName[0] != '\0') {
    return "cta-maintd:" + std::string(myName) + ":" + hostName;
  }
  return "cta-maintd:" + hostName;
}

}  // namespace

MountDecisionRoutine::MountDecisionRoutine(cta::log::LogContext& lc,
                                           cta::ConnProvider& connectionProvider,
                                           cta::SchedulerDatabase& schedulerDb,
                                           cta::Scheduler& scheduler)
    : m_lc(lc),
      m_scheduler(scheduler),
      m_counterKey(getMountDecisionCounterKey()),
      m_mountDecision(connectionProvider, schedulerDb, lc) {
  log::ScopedParamContainer params(m_lc);
  params.add("mountDecisionCounterKey", m_counterKey);
  m_lc.log(cta::log::INFO, "In MountDecisionRoutine: Created MountDecisionRoutine");
}

void MountDecisionRoutine::execute() {
  if (m_mountDecision.incrementCounter(m_counterKey, m_lc, "maintenanceLoop")) {
    log::ScopedParamContainer params(m_lc);
    params.add("mountDecisionCounterKey", m_counterKey);
    m_lc.log(log::DEBUG, "In MountDecisionRoutine::execute(): Incremented loop counter.");
  }
  m_mountDecision.refreshMountCandidates(m_counterKey, m_scheduler, m_lc);
}

std::string MountDecisionRoutine::getName() const {
  return "MountDecisionRoutine";
}

}  // namespace cta::maintd
