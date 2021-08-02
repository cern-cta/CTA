/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "RepackReportThread.hpp"
namespace cta {

  RepackReportThread::~RepackReportThread() {
  }
  
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