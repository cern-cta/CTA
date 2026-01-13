/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AncientRowRoutines.hpp"

namespace cta::maintd {

DeleteOldFailedQueuesRoutine::DeleteOldFailedQueuesRoutine(log::LogContext& lc,
                                                           RelationalDB& pgs,
                                                           size_t batchSize,
                                                           uint64_t inactiveTimeLimit)
    : m_lc(lc),
      m_RelationalDB(pgs),
      m_batchSize(batchSize),
      m_inactiveTimeLimit(inactiveTimeLimit) {
  log::ScopedParamContainer params(m_lc);
  params.add("batchSize", m_batchSize);
  params.add("inactiveTimeLimit", m_inactiveTimeLimit);
  m_lc.log(cta::log::INFO, "Created " + std::string(m_routineName));
};

CleanMountLastFetchTimeRoutine::CleanMountLastFetchTimeRoutine(log::LogContext& lc,
                                                               RelationalDB& pgs,
                                                               size_t batchSize,
                                                               uint64_t inactiveTimeLimit)
    : m_lc(lc),
      m_RelationalDB(pgs),
      m_batchSize(batchSize),
      m_inactiveTimeLimit(inactiveTimeLimit) {
  log::ScopedParamContainer params(m_lc);
  params.add("batchSize", m_batchSize);
  params.add("inactiveTimeLimit", m_inactiveTimeLimit);
  m_lc.log(cta::log::INFO, "Created " + std::string(m_routineName));
};

void DeleteOldFailedQueuesRoutine::execute() {
  // only rows older than 2 weeks will be deleted from the FAILED tables
  m_RelationalDB.deleteOldFailedQueues(m_inactiveTimeLimit, m_batchSize, m_lc);
};

void CleanMountLastFetchTimeRoutine::execute() {
  // Cleaning MOUNT_QUEUE_LAST_FETCH table from MOUNT IDs which were inactive for more than m_inactiveTimeLimit
  m_RelationalDB.cleanOldMountLastFetchTimes(m_inactiveTimeLimit, m_batchSize, m_lc);
};

}  // namespace cta::maintd
