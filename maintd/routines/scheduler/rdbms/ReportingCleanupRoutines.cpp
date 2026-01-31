/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ReportingCleanupRoutines.hpp"

namespace cta::maintd {

ResubmitInactiveReportingRoutine::ResubmitInactiveReportingRoutine(log::LogContext& lc,
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

void ResubmitInactiveReportingRoutine::execute() {
  // Updated IS_REPORTING state back to false to make rows eligible to be picked up for reporting again if in case of dead reporting process.
  m_RelationalDB.resubmitInactiveReporting(m_inactiveTimeLimit, m_batchSize, m_lc);
};

}  // namespace cta::maintd
