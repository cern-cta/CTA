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

#include <memory>
#include <numeric>
#include <cstdio>

#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/utils/utils.hpp"
#include "common/Timer.hpp"

using cta::log::LogContext;
using cta::log::Param;

namespace castor::tape::tapeserver::daemon {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MigrationReportPacker::MigrationReportPacker(cta::ArchiveMount* archiveMount, const cta::log::LogContext& lc)
    : ReportPackerInterface<detail::Migration>(lc),
      m_workerThread(*this),
      m_archiveMount(archiveMount) {}

//------------------------------------------------------------------------------
//Destructor
//------------------------------------------------------------------------------
MigrationReportPacker::~MigrationReportPacker() {
  cta::threading::MutexLocker ml(m_producterProtection);
  try {
    cta::log::ScopedParamContainer params(m_lc);
    if (m_successfulArchiveJobs.size() > 0) {
      params.add("successfulJobsLostInQueue", m_successfulArchiveJobs.size());
    }
    if (m_skippedFiles.size() > 0) {
      params.add("skippedFilesLostInQueue", m_successfulArchiveJobs.size());
    }
    if (m_successfulArchiveJobs.size() > 0 || m_skippedFiles.size() > 0) {
      m_lc.log(cta::log::WARNING, "In MigrationReportPacker::~MigrationReportPacker(), still pending jobs to report.");
    }
  } catch (...) {}
}

//------------------------------------------------------------------------------
//reportCompletedJob
//------------------------------------------------------------------------------
void MigrationReportPacker::reportCompletedJob(std::unique_ptr<cta::ArchiveJob> successfulArchiveJob,
                                               cta::log::LogContext& lc) {
  std::unique_ptr<Report> rep(new ReportSuccessful(std::move(successfulArchiveJob)));
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportSuccessful");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportCompletedJob(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportSkippedJob
//------------------------------------------------------------------------------
void MigrationReportPacker::reportSkippedJob(std::unique_ptr<cta::ArchiveJob> skippedArchiveJob,
                                             const std::string& failure,
                                             cta::log::LogContext& lc) {
  std::string failureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() + " " + failure;
  std::unique_ptr<Report> rep(new ReportSkipped(std::move(skippedArchiveJob), failureLog));
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReporSkipped");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportSkippedJob(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportFailedJob
//------------------------------------------------------------------------------
void MigrationReportPacker::reportFailedJob(std::unique_ptr<cta::ArchiveJob> failedArchiveJob,
                                            const cta::exception::Exception& ex,
                                            cta::log::LogContext& lc) {
  std::string failureLog =
    cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() + " " + ex.getMessageValue();
  std::unique_ptr<Report> rep(new ReportError(std::move(failedArchiveJob), failureLog));
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportError");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportFailedJob(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportFlush
//------------------------------------------------------------------------------
void MigrationReportPacker::reportFlush(drive::compressionStats compressStats, cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportFlush");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportFlush(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  std::unique_ptr<Report> rep(new ReportFlush(compressStats));
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportTapeFull
//------------------------------------------------------------------------------
void MigrationReportPacker::reportTapeFull(cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportTapeFull");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportTapeFull(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  std::unique_ptr<Report> rep(new ReportTapeFull());
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportErrorLastBatch
//------------------------------------------------------------------------------
void MigrationReportPacker::reportLastBatchError(const cta::exception::Exception& ex, cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer params(lc);
  std::string failureLog =
    cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() + " " + ex.getMessageValue();
  params.add("type", "ReportLastBatchError");
  lc.log(cta::log::INFO, "In MigrationReportPacker::reportLastBatchError(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  std::unique_ptr<Report> rep(new ReportLastBatchError(failureLog));
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportEndOfSession
//------------------------------------------------------------------------------
void MigrationReportPacker::reportEndOfSession(cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportEndofSession");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportEndOfSession(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  std::unique_ptr<Report> rep(new ReportEndofSession());
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportEndOfSessionWithErrors
//------------------------------------------------------------------------------
void MigrationReportPacker::reportEndOfSessionWithErrors(const std::string& msg,
                                                         bool isTapeFull,
                                                         cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportEndofSessionWithErrors");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportEndOfSessionWithErrors(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  std::unique_ptr<Report> rep(new ReportEndofSessionWithErrors(msg, isTapeFull));
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportTestGoingToEnd
//------------------------------------------------------------------------------
void MigrationReportPacker::reportTestGoingToEnd(cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportTestGoingToEnd");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportTestGoingToEnd(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  std::unique_ptr<Report> rep(new ReportTestGoingToEnd());
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//ReportSuccessful::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportSuccessful::execute(MigrationReportPacker& reportPacker) {
  reportPacker.m_successfulArchiveJobs.push(std::move(m_successfulArchiveJob));
}

//------------------------------------------------------------------------------
//reportSkipped:execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportSkipped::execute(MigrationReportPacker& reportPacker) {
  // We have no successful file to add, but we should report the failure for the file.
  {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("failureLog", m_failureLog)
      .add("fileSize", m_skippedArchiveJob->archiveFile.fileSize)
      .add("fileId", m_skippedArchiveJob->archiveFile.archiveFileID);
    m_skippedArchiveJob->archiveFile.checksumBlob.addFirstChecksumToLog(params);

    reportPacker.m_lc.log(cta::log::DEBUG,
                          "In MigrationReportPacker::ReportSkipped::execute(): skipping archive job after exception.");
  }
  try {
    m_skippedArchiveJob->transferFailed(m_failureLog, reportPacker.m_lc);
  } catch (cta::exception::NoSuchObject& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("ExceptionMSG", ex.getMessageValue()).add("fileId", m_skippedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::WARNING,
                          "In MigrationReportPacker::ReportSkipped::execute(): call to m_failedArchiveJob->failed(), "
                          "job does not exist in the objectstore.");
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("ExceptionMSG", ex.getMessageValue()).add("fileId", m_skippedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(
      cta::log::ERR,
      "In MigrationReportPacker::ReportSkipped::execute(): call to m_failedArchiveJob->failed() threw an exception.");
    reportPacker.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
  }
  reportPacker.m_skippedFiles.push(cta::catalogue::TapeItemWritten());
  auto& tapeItem = reportPacker.m_skippedFiles.back();
  tapeItem.fSeq = m_skippedArchiveJob->tapeFile.fSeq;
  tapeItem.tapeDrive = reportPacker.m_archiveMount->getDrive();
  tapeItem.vid = m_skippedArchiveJob->tapeFile.vid;
}

//------------------------------------------------------------------------------
//reportDriveStatus
//------------------------------------------------------------------------------
void MigrationReportPacker::reportDriveStatus(cta::common::dataStructures::DriveStatus status,
                                              const std::optional<std::string>& reason,
                                              cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportDriveStatus").add("Status", cta::common::dataStructures::toString(status));
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportDriveStatus(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  std::unique_ptr<Report> rep(new ReportDriveStatus(status, reason));
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//ReportDriveStatus::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportDriveStatus::execute(MigrationReportPacker& parent) {
  cta::log::ScopedParamContainer params(parent.m_lc);
  params.add("status", cta::common::dataStructures::toString(m_status));
  parent.m_lc.log(cta::log::DEBUG, "In MigrationReportPacker::ReportDriveStatus::execute(): reporting drive status.");
  parent.m_archiveMount->setDriveStatus(m_status, m_reason);
}

//------------------------------------------------------------------------------
//ReportLastBatchError::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportLastBatchError::execute(MigrationReportPacker& reportPacker) {
  // in case there are no remaining jobs, we refrain from sending an empty report to the client in this case.
  if (reportPacker.m_successfulArchiveJobs.empty() && reportPacker.m_skippedFiles.empty()) {
    reportPacker.m_lc.log(cta::log::INFO,
                          "Received a request to requeue last non-flushed job batch from tape session, but no jobs were found. Doing nothing.");
    return;
  }
  // We re-queue all the jobs which were left in the m_successfulArchiveJobs
  // after exception was thrown (no flush() could be called for these)
  std::unique_ptr<cta::ArchiveJob> job;
  std::list<std::string> jobIDsList;
  uint64_t njobstorequeue = reportPacker.m_successfulArchiveJobs.size();
  while (!reportPacker.m_successfulArchiveJobs.empty()) {
    job = std::move(reportPacker.m_successfulArchiveJobs.front());
    reportPacker.m_successfulArchiveJobs.pop();
    if (!job) {
      continue;
    }
    try {
      jobIDsList.emplace_back(job->getJobID());
    } catch (cta::exception::Exception& ex) {
      cta::log::ScopedParamContainer params(reportPacker.m_lc);
      params.add("ExceptionMSG", ex.getMessageValue())
            .add("archiveFileId", job->archiveFile.archiveFileID)
            .add("jobIDsListSize", jobIDsList.size());
      reportPacker.m_lc.log(cta::log::ERR,
                            "In MigrationReportPacker::ReportLastBatchError::execute(): looping through reportPacker "
                            "jobIDs threw an exception.");
      reportPacker.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
    }
  }
  try {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("reportPackerJobsToRequeue", njobstorequeue);
    uint64_t nrows = reportPacker.m_archiveMount->requeueJobBatch(jobIDsList, reportPacker.m_lc);
    params.add("jobsToRequeud", nrows);
    if (njobstorequeue != nrows) {
      reportPacker.m_lc.log(
        cta::log::ERR,
        "In MigrationReportPacker::ReportLastBatchError::execute(): requeueJobBatch() call failed, the "
        "reportPacker job count to requeue did not match the final requeued job count.");
    } else {
      reportPacker.m_lc.log(
        cta::log::INFO,
        "In MigrationReportPacker::ReportLastBatchError::execute(): requeueJobBatch() call succeeded.");
    }
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("ExceptionMSG", ex.getMessageValue()).add("reportPackerJobsToRequeue", njobstorequeue);
    reportPacker.m_lc.log(
      cta::log::ERR,
      "In MigrationReportPacker::ReportLastBatchError::execute(): call to requeueJobBatch threw an exception.");
    reportPacker.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
  }
}

//------------------------------------------------------------------------------
//ReportFlush::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportFlush::execute(MigrationReportPacker& reportPacker) {
  if (!reportPacker.m_errorHappened) {
    // We can receive double flushes when the periodic flush happens
    // right before the end of session (which triggers also a flush)
    // We refrain from sending an empty report to the client in this case.
    if (reportPacker.m_successfulArchiveJobs.empty() && reportPacker.m_skippedFiles.empty()) {
      reportPacker.m_lc.log(cta::log::INFO,
                            "Received a flush report from tape, but had no file to report to client. Doing nothing.");
      return;
    }
    std::queue<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>> failedToReportArchiveJobs;
    try {
      cta::utils::Timer t;
      cta::log::ScopedParamContainer params(reportPacker.m_lc);
      params.add("successfulBatchSize", reportPacker.m_successfulArchiveJobs.size());
      reportPacker.m_archiveMount->reportJobsBatchTransferred(reportPacker.m_successfulArchiveJobs,
                                                              reportPacker.m_skippedFiles,
                                                              failedToReportArchiveJobs,
                                                              reportPacker.m_lc);
      params.add("reportJobsBatchTime", t.secs()).add("failedToReportBatchSize", failedToReportArchiveJobs.size());
      reportPacker.m_lc.log(
        cta::log::INFO,
        "In MigrationReportPacker::ReportFlush::execute(): successfully reported batch of archive jobs to disk.");
    } catch (const cta::ArchiveMount::FailedReportCatalogueUpdate& ex) {
      while (!failedToReportArchiveJobs.empty()) {
        auto archiveJob = std::move(failedToReportArchiveJobs.front());
        try {
          archiveJob->failTransfer(ex.getMessageValue(), reportPacker.m_lc);
        } catch (const cta::exception::NoSuchObject& nso_ex) {
          cta::log::ScopedParamContainer params(reportPacker.m_lc);
          params.add("fileId", archiveJob->archiveFile.archiveFileID)
            .add("latestError", archiveJob->latestError)
            .add("exceptionMSG", ex.getMessageValue());
          reportPacker.m_lc.log(cta::log::WARNING,
                                "In MigrationReportPacker::ReportFlush::execute(): failed to failTransfer for the "
                                "archive job because it does not exist in the objectstore.");
        } catch (const cta::exception::Exception& cta_ex) {
          //If the failTransfer method fails, we can't do anything about it
          cta::log::ScopedParamContainer params(reportPacker.m_lc);
          params.add("fileId", archiveJob->archiveFile.archiveFileID)
            .add("latestError", archiveJob->latestError)
            .add("exceptionMSG", ex.getMessageValue());
          reportPacker.m_lc.log(cta::log::WARNING,
                                "In MigrationReportPacker::ReportFlush::execute(): failed to failTransfer for the "
                                "archive job because of CTA exception.");
        }
        failedToReportArchiveJobs.pop();
      }
      throw;
    } catch (const cta::ArchiveMount::FailedReportMoveToQueue& ex) {
      while (!failedToReportArchiveJobs.empty()) {
        auto archiveJob = std::move(failedToReportArchiveJobs.front());
        try {
          archiveJob->failReport(ex.getMessageValue(), reportPacker.m_lc);
        } catch (const cta::exception::NoSuchObject& nso_ex) {
          cta::log::ScopedParamContainer params(reportPacker.m_lc);
          params.add("fileId", archiveJob->archiveFile.archiveFileID)
            .add("latestError", archiveJob->latestError)
            .add("exceptionMSG", ex.getMessageValue());
          reportPacker.m_lc.log(cta::log::WARNING,
                                "In MigrationReportPacker::ReportFlush::execute(): failed to failReport for the "
                                "archive job because it does not exist in the objectstore.");
        } catch (const cta::exception::Exception& cta_ex) {
          //If the failReport method fails, we can't do anything about it
          cta::log::ScopedParamContainer params(reportPacker.m_lc);
          params.add("fileId", archiveJob->archiveFile.archiveFileID)
            .add("latestError", archiveJob->latestError)
            .add("exceptionMSG", ex.getMessageValue());
          reportPacker.m_lc.log(cta::log::WARNING,
                                "In MigrationReportPacker::ReportFlush::execute(): failed to failReport for the "
                                "archive job because of CTA exception.");
        }
        failedToReportArchiveJobs.pop();
      }
      throw;
    }
  } else {
    // This is an abnormal situation: we should never flush after an error!
    reportPacker.m_lc.log(cta::log::ALERT, "Received a flush after an error: sending file errors to client");
  }
}

//------------------------------------------------------------------------------
//reportTapeFull()::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportTapeFull::execute(MigrationReportPacker& reportPacker) {
  reportPacker.m_archiveMount->setTapeFull();
}

//------------------------------------------------------------------------------
//ReportEndofSession::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSession::execute(MigrationReportPacker& reportPacker) {
  reportPacker.m_continue = false;
  reportPacker.m_lc.log(cta::log::DEBUG,
                        "In MigrationReportPacker::ReportEndofSession::execute(): reporting session complete.");
  reportPacker.m_archiveMount->complete();
  if (!reportPacker.m_errorHappened) {
    cta::log::ScopedParamContainer sp(reportPacker.m_lc);
    reportPacker.m_lc.log(cta::log::INFO, "Reported end of session to client");
    if (reportPacker.m_watchdog) {
      reportPacker.m_watchdog->addParameter(cta::log::Param("status", "success"));
      // We have a race condition here between the processing of this message by
      // the initial process and the printing of the end-of-session log, triggered
      // by the end our process. To delay the latter, we sleep half a second here.
      usleep(500 * 1000);
    }
  } else {
    // We have some errors
    cta::log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add("errorMessage", "Previous file errors");
    reportPacker.m_lc.log(cta::log::ERR, "Reported end of session with error to client due to previous file errors");
    if (reportPacker.m_watchdog) {
      reportPacker.m_watchdog->addParameter(cta::log::Param("status", "failure"));
      // We have a race condition here between the processing of this message by
      // the initial process and the printing of the end-of-session log, triggered
      // by the end our process. To delay the latter, we sleep half a second here.
      usleep(500 * 1000);
    }
  }
}

//------------------------------------------------------------------------------
//ReportEndofSessionWithErrors::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSessionWithErrors::execute(MigrationReportPacker& reportPacker) {
  reportPacker.m_continue = false;
  reportPacker.m_lc.log(
    cta::log::DEBUG,
    "In MigrationReportPacker::ReportEndofSessionWithErrors::execute(): reporting session complete.");
  reportPacker.m_archiveMount->complete();
  if (reportPacker.m_errorHappened) {
    cta::log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add("errorMessage", m_message).add("isTapeFull", m_isTapeFull);
    reportPacker.m_lc.log(cta::log::INFO, "Reported end of session with error to client after sending file errors");
  } else {
    reportPacker.m_lc.log(cta::log::INFO, "Reported end of session with error to client");
  }
  if (reportPacker.m_watchdog) {
    reportPacker.m_watchdog->addParameter(cta::log::Param("status", m_isTapeFull ? "success" : "failure"));
    // We have a race condition here between the processing of this message by
    // the initial process and the printing of the end-of-session log, triggered
    // by the end our process. To delay the latter, we sleep half a second here.
    usleep(500 * 1000);
  }
}

//------------------------------------------------------------------------------
//ReportError::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportError::execute(MigrationReportPacker& reportPacker) {
  reportPacker.m_errorHappened = true;
  {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("failureLog", m_failureLog).add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::ERR,
                          "In MigrationReportPacker::ReportError::execute(): failing archive job after exception.");
  }
  try {
    m_failedArchiveJob->transferFailed(m_failureLog, reportPacker.m_lc);
  } catch (cta::exception::NoSuchObject& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("ExceptionMSG", ex.getMessageValue()).add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::WARNING,
                          "In MigrationReportPacker::ReportError::execute(): call to m_failedArchiveJob->failed(), job "
                          "does not exist in the objectstore.");
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("ExceptionMSG", ex.getMessageValue()).add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(
      cta::log::ERR,
      "In MigrationReportPacker::ReportError::execute(): call to m_failedArchiveJob->failed() threw an exception.");
    reportPacker.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
  }
}

//------------------------------------------------------------------------------
//WorkerThread::WorkerThread
//------------------------------------------------------------------------------
MigrationReportPacker::WorkerThread::WorkerThread(MigrationReportPacker& parent) : m_parent(parent) {}

//------------------------------------------------------------------------------
//WorkerThread::run
//------------------------------------------------------------------------------
void MigrationReportPacker::WorkerThread::run() {
  // Create our own log context for the new thread.
  cta::log::LogContext lc = m_parent.m_lc;
  lc.pushOrReplace(cta::log::Param("thread", "ReportPacker"));
  try {
    while (m_parent.m_continue) {
      std::unique_ptr<Report> rep(m_parent.m_fifo.pop());
      {
        cta::log::ScopedParamContainer params(lc);
        int demangleStatus;
        char* demangledReportType = abi::__cxa_demangle(typeid(*rep.get()).name(), nullptr, nullptr, &demangleStatus);
        if (!demangleStatus) {
          params.add("typeId", demangledReportType);
        } else {
          params.add("typeId", typeid(*rep.get()).name());
        }
        free(demangledReportType);
        lc.log(cta::log::DEBUG, "In MigrationReportPacker::WorkerThread::run(): Got a new report.");
      }
      rep->execute(m_parent);
    }
  } catch (const cta::exception::Exception& e) {
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMSG", e.getMessageValue());
    lc.log(
      cta::log::ERR,
      "In MigrationReportPacker::WorkerThread::run(): Received a CTA exception while reporting archive mount results.");
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_reporting");
      m_parent.m_watchdog->addParameter(cta::log::Param("status", "failure"));
    }
  } catch (const std::exception& e) {
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMSG", e.what());
    int demangleStatus;
    char* demangleExceptionType = abi::__cxa_demangle(typeid(e).name(), nullptr, nullptr, &demangleStatus);
    if (!demangleStatus) {
      params.add("exceptionType", demangleExceptionType);
    } else {
      params.add("exceptionType", typeid(e).name());
    }
    lc.log(cta::log::ERR,
           "In MigrationReportPacker::WorkerThread::run(): Received a standard exception while reporting archive mount "
           "results.");
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_reporting");
      m_parent.m_watchdog->addParameter(cta::log::Param("status", "failure"));
    }
  } catch (...) {
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    lc.log(cta::log::ERR,
           "In MigrationReportPacker::WorkerThread::run(): Received an unknown exception while reporting archive mount "
           "results.");
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_reporting");
      m_parent.m_watchdog->addParameter(cta::log::Param("status", "failure"));
    }
  }
  // Drain the FIFO if necessary. We know that m_continue will be
  // set by ReportEndofSessionWithErrors or ReportEndofSession
  // TODO devise a more generic mechanism
  uint64_t leftOverReportCount = m_parent.m_fifo.size();
  if (leftOverReportCount != 0) {
    cta::log::ScopedParamContainer params(lc);
    params.add("leftOverReportCount", leftOverReportCount);
    params.add("MigrationReportPacker.m_continue", m_parent.m_continue);
    lc.log(cta::log::ERR, "In MigrationReportPacker::WorkerThread::run(): leftover reports will not get executed !");
  }
  while (m_parent.m_fifo.size()) {
    std::unique_ptr<Report> rep(m_parent.m_fifo.pop());
    cta::log::ScopedParamContainer params(lc);
    int demangleStatus;
    char* demangledReportType = abi::__cxa_demangle(typeid(*rep.get()).name(), nullptr, nullptr, &demangleStatus);
    if (!demangleStatus) {
      params.add("typeId", demangledReportType);
    } else {
      params.add("typeId", typeid(*rep.get()).name());
    }
    free(demangledReportType);
    lc.log(cta::log::DEBUG, "In MigrationReportPacker::WorkerThread::run(): Draining leftover.");
  }
}

}  // namespace castor::tape::tapeserver::daemon
