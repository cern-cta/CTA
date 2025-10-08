/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "Scheduler.hpp"
#include "common/threading/Thread.hpp"

namespace cta {

class RepackReportThread: public cta::threading::Thread {
public:
  RepackReportThread(Scheduler& scheduler, log::LogContext &lc):m_scheduler(scheduler),m_lc(lc){}
  virtual ~RepackReportThread() = default;
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
