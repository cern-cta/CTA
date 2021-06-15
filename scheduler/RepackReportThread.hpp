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
#pragma once

#include "Scheduler.hpp"
#include "common/threading/Thread.hpp"

namespace cta {
  
class RepackReportThread: public cta::threading::Thread {
public:
  RepackReportThread(Scheduler& scheduler, log::LogContext &lc):m_scheduler(scheduler),m_lc(lc){}
  virtual ~RepackReportThread();
  void run();
protected:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext &lc) = 0;
  virtual std::string getReportingType() = 0;
  Scheduler& m_scheduler;
  log::LogContext& m_lc;
  const double c_maxTimeToReport = 30.0;
};

class RetrieveSuccessesRepackReportThread: public RepackReportThread{
public:
  RetrieveSuccessesRepackReportThread(Scheduler& scheduler,log::LogContext& lc):RepackReportThread(scheduler,lc) {}
private:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext &lc);
  virtual std::string getReportingType(){ return "RetrieveSuccesses"; }
};

class ArchiveSuccessesRepackReportThread: public RepackReportThread{
public:
  ArchiveSuccessesRepackReportThread(Scheduler& scheduler,log::LogContext& lc):RepackReportThread(scheduler,lc) {}
private:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext &lc);
  virtual std::string getReportingType(){ return "ArchiveSuccesses"; }
};

class RetrieveFailedRepackReportThread: public RepackReportThread{
public:
  RetrieveFailedRepackReportThread(Scheduler& scheduler,log::LogContext& lc):RepackReportThread(scheduler,lc) {}
private:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext &lc);
  virtual std::string getReportingType(){ return "RetrieveFailed"; }
};

class ArchiveFailedRepackReportThread: public RepackReportThread{
public:
  ArchiveFailedRepackReportThread(Scheduler& scheduler,log::LogContext& lc):RepackReportThread(scheduler,lc) {}
private:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext &lc);
  virtual std::string getReportingType(){ return "ArchiveFailed"; }
};

}