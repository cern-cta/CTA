/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */
#pragma once

#include "Scheduler.hpp"
#include "common/threading/Thread.hpp"

namespace cta {

class RepackReportThread : public cta::threading::Thread {
public:
  RepackReportThread(Scheduler& scheduler, log::LogContext& lc) : m_scheduler(scheduler), m_lc(lc) {}

  virtual ~RepackReportThread();
  void run();

protected:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext& lc) = 0;
  virtual std::string getReportingType() = 0;
  Scheduler& m_scheduler;
  log::LogContext& m_lc;
  const double c_maxTimeToReport = 30.0;
};

class RetrieveSuccessesRepackReportThread : public RepackReportThread {
public:
  RetrieveSuccessesRepackReportThread(Scheduler& scheduler, log::LogContext& lc) : RepackReportThread(scheduler, lc) {}

private:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext& lc);

  virtual std::string getReportingType() { return "RetrieveSuccesses"; }
};

class ArchiveSuccessesRepackReportThread : public RepackReportThread {
public:
  ArchiveSuccessesRepackReportThread(Scheduler& scheduler, log::LogContext& lc) : RepackReportThread(scheduler, lc) {}

private:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext& lc);

  virtual std::string getReportingType() { return "ArchiveSuccesses"; }
};

class RetrieveFailedRepackReportThread : public RepackReportThread {
public:
  RetrieveFailedRepackReportThread(Scheduler& scheduler, log::LogContext& lc) : RepackReportThread(scheduler, lc) {}

private:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext& lc);

  virtual std::string getReportingType() { return "RetrieveFailed"; }
};

class ArchiveFailedRepackReportThread : public RepackReportThread {
public:
  ArchiveFailedRepackReportThread(Scheduler& scheduler, log::LogContext& lc) : RepackReportThread(scheduler, lc) {}

private:
  virtual cta::Scheduler::RepackReportBatch getNextRepackReportBatch(log::LogContext& lc);

  virtual std::string getReportingType() { return "ArchiveFailed"; }
};

}  // namespace cta