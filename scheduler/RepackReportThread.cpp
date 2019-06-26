/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "RepackReportThread.hpp"
namespace cta {

  RepackReportThread::~RepackReportThread() {
  }
  
  void RepackReportThread::run() {
    m_runTimer.reset();
    bool moreBatch = true;
    while(m_runTimer.secs() < 30.0 && moreBatch){
      cta::Scheduler::RepackReportBatch reportBatch = getNextRepackReportBatch(m_lc);
      if(!reportBatch.empty()){
        reportBatch.report(m_lc);
      } else {
        moreBatch = false;
      }
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