/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RepackReportRoutine.hpp"

#include "common/exception/NoSuchObject.hpp"
#include "common/semconv/Attributes.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::maintd {

RepackReportRoutine::RepackReportRoutine(cta::log::LogContext& lc, cta::Scheduler& scheduler, int timeout)
    : m_lc(lc),
      m_scheduler(scheduler),
      m_softTimeout(timeout) {
  log::ScopedParamContainer params(m_lc);
  params.add("softTimeout", timeout);
  m_lc.log(cta::log::INFO, "Created RepackReportRoutine");
}

void RepackReportRoutine::execute() {
  reportBatch(cta::semconv::attr::CtaRepackReportTypeValues::kRetrieveSuccess,
              [this] { return m_scheduler.getNextSuccessfulRetrieveRepackReportBatch(m_lc); });
  reportBatch(cta::semconv::attr::CtaRepackReportTypeValues::kArchiveSuccess,
              [this] { return m_scheduler.getNextSuccessfulArchiveRepackReportBatch(m_lc); });
  reportBatch(cta::semconv::attr::CtaRepackReportTypeValues::kRetrieveFailed,
              [this] { return m_scheduler.getNextFailedRetrieveRepackReportBatch(m_lc); });
  reportBatch(cta::semconv::attr::CtaRepackReportTypeValues::kArchiveFailed,
              [this] { return m_scheduler.getNextFailedArchiveRepackReportBatch(m_lc); });
}

template<typename GetBatchFunc>
void RepackReportRoutine::reportBatch(const std::string& reportingType, GetBatchFunc getBatchFunc) const {
  utils::Timer totalTime;
  log::ScopedParamContainer params(m_lc);
  params.add("repackReportType", reportingType);

  uint64_t numberOfBatchReported = 0;

  while (totalTime.secs() < m_softTimeout) {
    utils::Timer t;
    log::TimingList tl;

    cta::Scheduler::RepackReportBatch reportBatch = getBatchFunc();
    tl.insertAndReset("getNextRepackReportBatchTime", t);

    if (reportBatch.empty()) {
      // Nothing to do
      break;
    }

    reportBatch.report(m_lc);
    numberOfBatchReported++;
    tl.insertAndReset("reportingTime", t);

    log::ScopedParamContainer paramsReport(m_lc);
    tl.addToLog(paramsReport);
    m_lc.log(log::DEBUG, "In RepackReportRoutine::reportBatch(), reported a batch of reports.");
  }

  if (numberOfBatchReported > 0) {
    params.add("batchesReported", numberOfBatchReported);
    params.add("duration", totalTime.secs());
    m_lc.log(log::INFO, "In RepackReportRoutine::reportBatch(): finished one pass for " + reportingType + ".");
  }
}

std::string RepackReportRoutine::getName() const {
  return "RepackReportRoutine";
}

}  // namespace cta::maintd
