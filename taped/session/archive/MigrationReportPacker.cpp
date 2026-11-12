/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MigrationReportPacker.hpp"

#include "catalogue/TapeFileWritten.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "taped/drive/DriveInterface.hpp"

#include <cstdio>
#include <memory>
#include <numeric>
#include <stdexcept>

using cta::log::LogContext;
using cta::log::Param;

namespace cta::tape::daemon {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MigrationReportPacker::MigrationReportPacker(cta::ArchiveMount* archiveMount,
                                             const cta::log::LogContext& lc,
                                             TapeSessionTracker& tracker)
    : ReportPackerInterface(lc, tracker),
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
//reportFileNotArchived
//------------------------------------------------------------------------------
void MigrationReportPacker::reportFileNotArchived(std::unique_ptr<cta::ArchiveJob> failedArchiveJob,
                                                  const std::string& failure,
                                                  cta::log::LogContext& lc,
                                                  RecordedFailure recordedFailure) {
  if (!recordedFailure.belongsTo(m_tapeSessionTracker)) {
    throw std::logic_error("Failed-job report belongs to a different session tracker");
  }
  std::string failureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() + " " + failure;
  auto rep = std::make_unique<ReportFileNotArchived>(std::move(failedArchiveJob), failureLog);
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportFileNotArchived");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportFileNotArchived(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(std::move(rep));
}

//------------------------------------------------------------------------------
//reportFailedJob
//------------------------------------------------------------------------------
void MigrationReportPacker::reportFailedJob(std::unique_ptr<cta::ArchiveJob> failedArchiveJob,
                                            const cta::exception::Exception& ex,
                                            cta::log::LogContext& lc,
                                            RecordedFailure recordedFailure) {
  if (!recordedFailure.belongsTo(m_tapeSessionTracker)) {
    throw std::logic_error("Failed-job report belongs to a different session tracker");
  }
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
void MigrationReportPacker::reportEndOfSession(SessionEndReason reason, cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportEndofSession");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportEndOfSession(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  auto rep = std::make_unique<ReportEndofSession>(reason);
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
//ReportFileNotArchived::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportFileNotArchived::execute(MigrationReportPacker& reportPacker) {
  // We have no successful file to add, but we should report the failure for the file.
  {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("failureLog", m_failureLog)
      .add("fileSize", m_failedArchiveJob->archiveFile.fileSize)
      .add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    m_failedArchiveJob->archiveFile.checksumBlob.addFirstChecksumToLog(params);

    reportPacker.m_lc.log(cta::log::DEBUG,
                          "In MigrationReportPacker::ReportFileNotArchived::execute(): "
                          "reporting file not archived after exception.");
  }

  // Tape catalogue record: must be written before the job is potentially moved.
  reportPacker.m_skippedFiles.push(cta::catalogue::TapeItemWritten());
  auto& tapeItem = reportPacker.m_skippedFiles.back();
  tapeItem.fSeq = m_failedArchiveJob->tapeFile.fSeq;
  tapeItem.tapeDrive = reportPacker.m_archiveMount->getDrive();
  tapeItem.vid = m_failedArchiveJob->tapeFile.vid;

#ifdef CTA_PGSCHED
  // Batch path: defer the DB update; it will be flushed via flushFailedJobs().
  m_failedArchiveJob->prepareFailTransfer(m_failureLog);
  reportPacker.m_failedArchiveJobs.push_back(std::move(m_failedArchiveJob));
  if (reportPacker.m_failedArchiveJobs.size() >= MigrationReportPacker::c_failedJobsBatchMax) {
    reportPacker.flushFailedJobs(reportPacker.m_lc);
  }
#else
  // Objectstore path: update the scheduler DB immediately for this single job.
  try {
    m_failedArchiveJob->transferFailed(m_failureLog, reportPacker.m_lc);
  } catch (cta::exception::NoSuchObject& ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
      .add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(
      cta::log::WARNING,
      "In MigrationReportPacker::ReportFileNotArchived::execute(): call to m_failedArchiveJob->failed(), "
      "job does not exist in the objectstore.");
  } catch (cta::exception::Exception& ex) {
    reportPacker.m_tapeSessionTracker.recordFailure(TapeSessionFailure::Reporting);
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
      .add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::ERR,
                          "In MigrationReportPacker::ReportFileNotArchived::execute(): call to "
                          "m_failedArchiveJob->failed() threw an exception.");
    reportPacker.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
  }
#endif
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
      reportPacker.m_tapeSessionTracker.recordFailure(TapeSessionFailure::Reporting);
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
      reportPacker.m_tapeSessionTracker.recordFailure(TapeSessionFailure::Reporting);
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
    reportPacker.m_tapeSessionTracker.recordFailure(TapeSessionFailure::Reporting);
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
  if (!reportPacker.m_failedJobReported) {
    // We can receive double flushes when the periodic flush happens
    // right before the end of session (which triggers also a flush)
    // We refrain from sending an empty report to the client in this case.
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
  // Flush accumulated failed jobs on every tape flush event. NOT reached if
  // reportSuccessfulJobsBatch() throws in the branch above (normal, no prior error): that
  // exception propagates out of WorkerThread::run()'s loop, ending the reporting thread before
  // ReportEndofSession's own flushFailedJobs() call ever runs. See c_failedJobsBatchMax / GC.
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
  const char* reason = "completed";
  switch (m_reason) {
    case SessionEndReason::Completed:
      break;
    case SessionEndReason::TapeFull:
      reason = "tape full";
      break;
    case SessionEndReason::Failure:
      reason = "failure";
      break;
  }
  // A full tape does not erase independent transfer or reporting failures.
  const bool failed = m_reason == SessionEndReason::Failure || reportPacker.m_tapeSessionTracker.hasFailures();
  cta::log::ScopedParamContainer sp(reportPacker.m_lc);
  sp.add("terminationReason", reason).add("status", failed ? "error" : "success");
  reportPacker.m_lc.log(failed ? cta::log::ERR : cta::log::INFO, "Reported end of archive session to client");
}

//------------------------------------------------------------------------------
//ReportError::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportError::execute(MigrationReportPacker& reportPacker) {
  reportPacker.m_failedJobReported = true;
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
    reportPacker.m_tapeSessionTracker.recordFailure(TapeSessionFailure::Reporting);
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
#ifdef CTA_PGSCHED
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
#endif  // CTA_PGSCHED

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
    // Catalogue update failed before any DB state changed: re-queue these jobs as transfer
    // failures. Pgsched batches them in one failTransferBatch() call; re-thrown below so the
    // caller can react (terminates the worker in the normal path, absorbed in error-recovery).
    // Objectstore updates each job individually (unchanged).
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
    // Catalogue update succeeded but the scheduler-DB queue-state transition failed: these jobs
    // are on tape and catalogued, so they go through the report-failure path, not transfer-
    // failure. Pgsched batches them in one failReportBatch() call; re-thrown for the same reason
    // as FailedReportCatalogueUpdate above. Objectstore updates each job individually (unchanged).
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
    m_parent.m_tapeSessionTracker.recordFailure(TapeSessionFailure::Reporting);
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
    m_parent.m_tapeSessionTracker.recordFailure(TapeSessionFailure::Reporting);
  } catch (...) {
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    lc.log(cta::log::ERR,
           "In MigrationReportPacker::WorkerThread::run(): Received an unknown exception while reporting archive mount "
           "results.");
    m_parent.m_tapeSessionTracker.recordFailure(TapeSessionFailure::Reporting);
  }
  // Drain the FIFO if necessary. We know that m_continue will be
  // set by ReportEndofSession
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
