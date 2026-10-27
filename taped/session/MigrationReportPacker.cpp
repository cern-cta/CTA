/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MigrationReportPacker.hpp"

#include "TaskWatchDog.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "taped/drive/DriveInterface.hpp"

#include <cstdio>
#include <memory>
#include <numeric>

using cta::log::LogContext;
using cta::log::Param;

namespace cta::tape::daemon {

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
#ifdef CTA_PGSCHED
    if (m_failedArchiveJobs.size() > 0) {
      params.add("failedJobsLostInQueue", m_failedArchiveJobs.size());
    }
#endif
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
  auto rep = std::make_unique<ReportSuccessful>(std::move(successfulArchiveJob));
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
  auto rep = std::make_unique<ReportSkipped>(std::move(skippedArchiveJob), failureLog);
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportSkipped");
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
  auto rep = std::make_unique<ReportError>(std::move(failedArchiveJob), failureLog);
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportError");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportFailedJob(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportFlush
//------------------------------------------------------------------------------
void MigrationReportPacker::reportFlush(const drive::compressionStats& compressStats, cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportFlush");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportFlush(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  auto rep = std::make_unique<ReportFlush>(compressStats);
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
  auto rep = std::make_unique<ReportTapeFull>();
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
  auto rep = std::make_unique<ReportLastBatchError>(failureLog);
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
  auto rep = std::make_unique<ReportEndofSession>();
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
  auto rep = std::make_unique<ReportEndofSessionWithErrors>(msg, isTapeFull);
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
  auto rep = std::make_unique<ReportTestGoingToEnd>();
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
// A skipped file is a zero-length file or one whose checksum failed: a placeholder
// fSeq was written to tape, so the tape catalogue record must still be written
// (m_skippedFiles), but the file itself counts as a transfer failure.
//
// Objectstore path: call transferFailed() immediately (individual DB update).
// Pgsched path:     call prepareFailTransfer() + accumulate into m_failedArchiveJobs
//                   so the failure is batched with all other transfer failures and
//                   flushed in a single failTransferBatch() call.
// The tape catalogue record (m_skippedFiles) is written unconditionally in both paths.
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportSkipped::execute(MigrationReportPacker& reportPacker) {
  {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("failureLog", m_failureLog)
      .add("fileSize", m_skippedArchiveJob->archiveFile.fileSize)
      .add("fileId", m_skippedArchiveJob->archiveFile.archiveFileID);
    m_skippedArchiveJob->archiveFile.checksumBlob.addFirstChecksumToLog(params);
    reportPacker.m_lc.log(cta::log::DEBUG,
                          "In MigrationReportPacker::ReportSkipped::execute(): skipping archive job after exception.");
  }

  // Tape catalogue record: must be written before the job is potentially moved.
  reportPacker.m_skippedFiles.push(cta::catalogue::TapeItemWritten());
  auto& tapeItem = reportPacker.m_skippedFiles.back();
  tapeItem.fSeq = m_skippedArchiveJob->tapeFile.fSeq;
  tapeItem.tapeDrive = reportPacker.m_archiveMount->getDrive();
  tapeItem.vid = m_skippedArchiveJob->tapeFile.vid;

#ifdef CTA_PGSCHED
  // Batch path: defer the DB update; it will be flushed via flushFailedJobs().
  m_skippedArchiveJob->prepareFailTransfer(m_failureLog);
  reportPacker.m_failedArchiveJobs.push_back(std::move(m_skippedArchiveJob));
  if (reportPacker.m_failedArchiveJobs.size() >= MigrationReportPacker::c_failedJobsBatchMax) {
    reportPacker.flushFailedJobs(reportPacker.m_lc);
  }
#else
  // Objectstore path: update the scheduler DB immediately for this single job.
  try {
    m_skippedArchiveJob->transferFailed(m_failureLog, reportPacker.m_lc);
  } catch (cta::exception::NoSuchObject& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
      .add("fileId", m_skippedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::WARNING,
                          "In MigrationReportPacker::ReportSkipped::execute(): call to transferFailed(), "
                          "job does not exist in the objectstore.");
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
      .add("fileId", m_skippedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(
      cta::log::ERR,
      "In MigrationReportPacker::ReportSkipped::execute(): call to transferFailed() threw an exception.");
    reportPacker.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
  }
#endif
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
  auto rep = std::make_unique<ReportDriveStatus>(status, reason);
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
                          "Received a request to requeue last non-flushed job batch from tape session, but no jobs "
                          "were found. Doing nothing.");
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
      params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
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
    params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
      .add("reportPackerJobsToRequeue", njobstorequeue);
    reportPacker.m_lc.log(
      cta::log::ERR,
      "In MigrationReportPacker::ReportLastBatchError::execute(): call to requeueJobBatch threw an exception.");
    reportPacker.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
  }
#ifdef CTA_PGSCHED
  reportPacker.flushFailedJobs(reportPacker.m_lc);
#endif
}

//------------------------------------------------------------------------------
//ReportFlush::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportFlush::execute(MigrationReportPacker& reportPacker) {
  if (!reportPacker.m_errorHappened) {
    // We can receive double flushes when the periodic flush happens right before end-of-session.
    // Refrain from sending an empty report to the client in this case.
    if (reportPacker.m_successfulArchiveJobs.empty() && reportPacker.m_skippedFiles.empty()) {
      reportPacker.m_lc.log(cta::log::INFO,
                            "Received a flush report from tape, but had no file to report to client. Doing nothing.");
      return;
    }
    reportPacker.reportSuccessfulJobsBatch(reportPacker.m_lc);
  } else {
    // In objectstore mode, a flush after an error is genuinely abnormal: the error path uses
    // reportLastBatchError (which re-queues pending successes) followed by reportEndOfSessionWithErrors,
    // not reportFlush. So this branch should never be reached in a correct objectstore session.
    reportPacker.m_lc.log(cta::log::ALERT, "Received a flush after an error: sending file errors to client");
#ifdef CTA_PGSCHED
    // In pgsched mode, individual file failures are soft — the session continues after them.
    // A flush after a failure is therefore normal; commit any pending successful jobs so they
    // are not lost alongside the failure batch.
    // Catch and log rather than re-throw: flushFailedJobs below must always run.
    try {
      reportPacker.reportSuccessfulJobsBatch(reportPacker.m_lc);
    } catch (const cta::exception::Exception& ex) {
      cta::log::ScopedParamContainer params(reportPacker.m_lc);
      params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
      reportPacker.m_lc.log(
        cta::log::ERR,
        "In MigrationReportPacker::ReportFlush::execute(): failed to report successful jobs in error-recovery path; "
        "proceeding to flush failed jobs.");
    }
#endif
  }
#ifdef CTA_PGSCHED
  // Piggyback: flush accumulated failed jobs on every tape flush event.
  // The only gap is failures that arrive after the last tape flush — those are held until
  // ReportEndofSession fires, which is guaranteed.
  reportPacker.flushFailedJobs(reportPacker.m_lc);
#endif
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
#ifdef CTA_PGSCHED
  reportPacker.flushFailedJobs(reportPacker.m_lc);
#endif
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
    sp.add(cta::semconv::log::exceptionMessage, "Previous file errors");
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
#ifdef CTA_PGSCHED
  reportPacker.flushFailedJobs(reportPacker.m_lc);
#endif
  reportPacker.m_archiveMount->complete();
  if (reportPacker.m_errorHappened) {
    cta::log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add(cta::semconv::log::exceptionMessage, m_message).add("isTapeFull", m_isTapeFull);
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
                          "In MigrationReportPacker::ReportError::execute(): accumulating failed archive job.");
  }
#ifdef CTA_PGSCHED
  m_failedArchiveJob->prepareFailTransfer(m_failureLog);
  reportPacker.m_failedArchiveJobs.push_back(std::move(m_failedArchiveJob));
  if (reportPacker.m_failedArchiveJobs.size() >= MigrationReportPacker::c_failedJobsBatchMax) {
    reportPacker.flushFailedJobs(reportPacker.m_lc);
  }
#else
  try {
    m_failedArchiveJob->transferFailed(m_failureLog, reportPacker.m_lc);
  } catch (cta::exception::NoSuchObject& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
      .add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::WARNING,
                          "In MigrationReportPacker::ReportError::execute(): call to m_failedArchiveJob->failed(), job "
                          "does not exist in the objectstore.");
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
      .add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(
      cta::log::ERR,
      "In MigrationReportPacker::ReportError::execute(): call to m_failedArchiveJob->failed() threw an exception.");
    reportPacker.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
  }
#endif
}

//------------------------------------------------------------------------------
// flushFailedJobs
//------------------------------------------------------------------------------
void MigrationReportPacker::flushFailedJobs(cta::log::LogContext& lc) {
  if (m_failedArchiveJobs.empty()) return;
  cta::log::ScopedParamContainer params(lc);
  params.add("failedBatchSize", m_failedArchiveJobs.size());
  lc.log(cta::log::INFO,
         "In MigrationReportPacker::flushFailedJobs(): flushing batch of failed archive jobs.");
  try {
    m_archiveMount->failTransferBatch(m_failedArchiveJobs, lc);
  } catch (const cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer p(lc);
    p.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    lc.log(cta::log::ERR,
           "In MigrationReportPacker::flushFailedJobs(): failTransferBatch threw; jobs will be rescued by GC.");
    lc.logBacktrace(cta::log::INFO, ex.backtrace());
  }
  m_failedArchiveJobs.clear();
}

//------------------------------------------------------------------------------
// reportSuccessfulJobsBatch
//------------------------------------------------------------------------------
void MigrationReportPacker::reportSuccessfulJobsBatch(cta::log::LogContext& lc) {
  if (m_successfulArchiveJobs.empty() && m_skippedFiles.empty()) return;
  std::queue<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>> failedToReportArchiveJobs;
  try {
    cta::utils::Timer t;
    cta::log::ScopedParamContainer params(lc);
    params.add("successfulBatchSize", m_successfulArchiveJobs.size());
    m_archiveMount->reportJobsBatchTransferred(m_successfulArchiveJobs, m_skippedFiles,
                                               failedToReportArchiveJobs, lc);
    params.add("reportJobsBatchTime", t.secs()).add("failedToReportBatchSize", failedToReportArchiveJobs.size());
    lc.log(cta::log::INFO,
           "In MigrationReportPacker::reportSuccessfulJobsBatch(): successfully reported batch of archive jobs to disk.");
  } catch (const cta::ArchiveMount::FailedReportCatalogueUpdate& ex) {
    // The catalogue update failed before any DB state was changed, so these jobs
    // have not been recorded anywhere and must be re-queued as transfer failures.
    //
    // Pgsched: batch all failedToReportArchiveJobs in one failTransferBatch() call
    //          (prepareFailTransfer is called inside the facade for each job).
    //          Re-throw so the caller (ReportFlush::execute) can react; in the normal
    //          path this terminates the worker; in the error-recovery path the caller's
    //          try/catch absorbs it so flushFailedJobs() still runs.
    //
    // Objectstore: update each job individually (existing behaviour unchanged).
#ifdef CTA_PGSCHED
    m_archiveMount->failTransferBatch(failedToReportArchiveJobs, ex.getMessageValue(), lc);
#else
    while (!failedToReportArchiveJobs.empty()) {
      auto archiveJob = std::move(failedToReportArchiveJobs.front());
      try {
        archiveJob->failTransfer(ex.getMessageValue(), lc);
      } catch (const cta::exception::NoSuchObject&) {
        cta::log::ScopedParamContainer params(lc);
        params.add("fileId", archiveJob->archiveFile.archiveFileID)
          .add("latestError", archiveJob->latestError)
          .add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
        lc.log(cta::log::WARNING,
               "In MigrationReportPacker::reportSuccessfulJobsBatch(): failed to failTransfer: job not in objectstore.");
      } catch (const cta::exception::Exception&) {
        cta::log::ScopedParamContainer params(lc);
        params.add("fileId", archiveJob->archiveFile.archiveFileID)
          .add("latestError", archiveJob->latestError)
          .add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
        lc.log(cta::log::WARNING,
               "In MigrationReportPacker::reportSuccessfulJobsBatch(): failed to failTransfer: CTA exception.");
      }
      failedToReportArchiveJobs.pop();
    }
#endif
    throw;
  } catch (const cta::ArchiveMount::FailedReportMoveToQueue& ex) {
    // The catalogue update succeeded but the scheduler-DB state update (setJobBatchTransferred)
    // failed.  These jobs are on tape and in the catalogue; only the queue-state transition
    // was lost.  They must go through the report-failure path (not transfer-failure).
    //
    // Pgsched: batch all failedToReportArchiveJobs in one failReportBatch() call
    //          (prepareFailReport is called inside the facade for each job).
    //          Jobs that have exhausted report retries go to ARCHIVE_FAILED_QUEUE;
    //          the rest have IS_REPORTING reset for the next report attempt.
    //          Re-throw for the same reason as FailedReportCatalogueUpdate above.
    //
    // Objectstore: update each job individually (existing behaviour unchanged).
#ifdef CTA_PGSCHED
    m_archiveMount->failReportBatch(failedToReportArchiveJobs, ex.getMessageValue(), lc);
#else
    while (!failedToReportArchiveJobs.empty()) {
      auto archiveJob = std::move(failedToReportArchiveJobs.front());
      try {
        archiveJob->failReport(ex.getMessageValue(), lc);
      } catch (const cta::exception::NoSuchObject&) {
        cta::log::ScopedParamContainer params(lc);
        params.add("fileId", archiveJob->archiveFile.archiveFileID)
          .add("latestError", archiveJob->latestError)
          .add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
        lc.log(cta::log::WARNING,
               "In MigrationReportPacker::reportSuccessfulJobsBatch(): failed to failReport: job not in objectstore.");
      } catch (const cta::exception::Exception&) {
        cta::log::ScopedParamContainer params(lc);
        params.add("fileId", archiveJob->archiveFile.archiveFileID)
          .add("latestError", archiveJob->latestError)
          .add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
        lc.log(cta::log::WARNING,
               "In MigrationReportPacker::reportSuccessfulJobsBatch(): failed to failReport: CTA exception.");
      }
      failedToReportArchiveJobs.pop();
    }
#endif
    throw;
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
  lc.push(cta::log::Param("thread", "ReportPacker"));
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
    params.add(cta::semconv::log::exceptionMessage, e.getMessageValue());
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
    params.add(cta::semconv::log::exceptionMessage, e.what());
    int demangleStatus;
    char* demangleExceptionType = abi::__cxa_demangle(typeid(e).name(), nullptr, nullptr, &demangleStatus);
    if (!demangleStatus) {
      params.add(cta::semconv::log::exceptionType, demangleExceptionType);
    } else {
      params.add(cta::semconv::log::exceptionType, typeid(e).name());
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
  if (uint64_t leftOverReportCount = m_parent.m_fifo.size(); leftOverReportCount != 0) {
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

}  // namespace cta::tape::daemon
