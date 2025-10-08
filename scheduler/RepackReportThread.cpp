/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RepackReportThread.hpp"
namespace cta {

  void RepackReportThread::run() {
    utils::Timer totalTime;
    bool moreBatch = true;
    log::ScopedParamContainer params(m_lc);
    params.add("reportingType",getReportingType());
    uint64_t numberOfBatchReported = 0;
    while(totalTime.secs() < c_maxTimeToReport && moreBatch){
      utils::Timer t;
      log::TimingList tl;
      cta::Scheduler::RepackReportBatch reportBatch = getNextRepackReportBatch(m_lc);
      tl.insertAndReset("getNextRepackReportBatchTime",t);
      if(!reportBatch.empty()) {
        reportBatch.report(m_lc);
        numberOfBatchReported++;
        tl.insertAndReset("reportingTime",t);
        log::ScopedParamContainer paramsReport(m_lc);
        tl.addToLog(paramsReport);
        m_lc.log(log::INFO,"In RepackReportThread::run(), reported a batch of reports.");
      } else {
        moreBatch = false;
      }
    }
    if(numberOfBatchReported > 0){
      params.add("numberOfBatchReported",numberOfBatchReported);
      params.add("totalRunTime",totalTime.secs());
      params.add("moreBatchToDo",moreBatch);
      m_lc.log(log::INFO,"In RepackReportThread::run(), exiting.");
    }
  }

  cta::Scheduler::RepackReportBatch RetrieveSuccessesRepackReportThread::getNextRepackReportBatch(log::LogContext &lc){
    return m_scheduler.getNextSuccessfulRetrieveRepackReportBatch(lc);
  }

  cta::Scheduler::RepackReportBatch ArchiveSuccessesRepackReportThread::getNextRepackReportBatch(log::LogContext &lc){
    return m_scheduler.getNextSuccessfulArchiveRepackReportBatch(lc);
  }

  cta::Scheduler::RepackReportBatch RetrieveFailedRepackReportThread::getNextRepackReportBatch(log::LogContext &lc){
    return m_scheduler.getNextFailedRetrieveRepackReportBatch(lc);
  }

  cta::Scheduler::RepackReportBatch ArchiveFailedRepackReportThread::getNextRepackReportBatch(log::LogContext &lc){
    return m_scheduler.getNextFailedArchiveRepackReportBatch(lc);
  }
}
