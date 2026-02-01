/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "QueueCleanupRoutine.hpp"

namespace cta::maintd {

QueueCleanupRoutine::QueueCleanupRoutine(cta::log::LogContext& lc,
                                         SchedulerDatabase& oStoreDb,
                                         catalogue::Catalogue& catalogue,
                                         int batchSize)
    : m_lc(lc),
      m_queueCleanup(lc, oStoreDb, catalogue, batchSize) {
  log::ScopedParamContainer params(m_lc);
  params.add("batchSize", batchSize);
  m_lc.log(cta::log::INFO, "In QueueCleanupRoutine: Created QueueCleanupRoutine");
}

void QueueCleanupRoutine::execute() {
  m_queueCleanup.cleanupQueues();
}

std::string QueueCleanupRoutine::getName() const {
  return "QueueCleanupRoutine";
}

}  // namespace cta::maintd
