/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MigrationTaskInjector.hpp"

#include "ErrorFlag.hpp"

#include <memory>

using cta::log::LogContext;
using cta::log::Param;

namespace cta::tape::daemon {

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
MigrationTaskInjector::MigrationTaskInjector(MigrationMemoryManager& mm,
                                             DiskReadThreadPool& diskReader,
                                             TapeSingleThreadInterface<TapeWriteTask>& tapeWriter,
                                             cta::ArchiveMount& archiveMount,
                                             uint64_t maxFiles,
                                             uint64_t byteSizeThreshold,
                                             uint64_t underfillWatchPeriodSecs,
                                             uint64_t underfillMinSamples,
                                             uint64_t underfillStartThreshold,
                                             uint64_t underfillRecoveryThreshold,
                                             const cta::log::LogContext& lc)
    : m_thread(*this),
      m_memManager(mm),
      m_tapeWriter(tapeWriter),
      m_diskReader(diskReader),
      m_archiveMount(archiveMount),
      m_lc(lc),
      m_maxFiles(maxFiles),
      m_maxBytes(byteSizeThreshold),
      m_underfillWatchPeriodSecs(underfillWatchPeriodSecs),
      m_underfillMinSamples(underfillMinSamples),
      m_underfillStartThreshold(underfillStartThreshold),
      m_underfillRecoveryThreshold(underfillRecoveryThreshold) {}

//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
void MigrationTaskInjector::injectBulkMigrations(std::list<std::unique_ptr<cta::ArchiveJob>>& jobs) {
  const size_t blockCapacity = m_memManager.blockCapacity();
  for (auto& job : jobs) {
    const uint64_t fileSize = job->archiveFile.fileSize;
    [[maybe_unused]] LogContext::ScopedParam sp[] = {
      LogContext::ScopedParam(m_lc, Param("fileId", job->archiveFile.archiveFileID)),
      LogContext::ScopedParam(m_lc, Param("fSeq", job->tapeFile.fSeq)),
      LogContext::ScopedParam(m_lc, Param("path", job->srcURL))};
    m_lc.log(cta::log::DEBUG, "MigrationTaskInjector::injectBulkMigrations file size: " + std::to_string(fileSize));
    const uint64_t neededBlock = howManyBlocksNeeded(fileSize, blockCapacity);

    // We give owner ship on the archive job to the tape write task (as last user).
    // disk read task gets a bare pointer.
    // TODO: could be changed as a shared_ptr.
    auto archiveJobPtr = job.get();
    auto twt = std::make_unique<TapeWriteTask>(neededBlock, job.release(), m_memManager, m_errorFlag);
    std::unique_ptr<DiskReadTask> drt;
    // We will skip the disk read task creation for zero-length files. Tape write task will handle the request and mark it as an
    // error.
    if (fileSize) {
      drt = std::make_unique<DiskReadTask>(*twt, archiveJobPtr, neededBlock, m_errorFlag);
    }

    m_tapeWriter.push(twt.release());
    m_lc.log(cta::log::INFO, "Created tasks for migrating a file");
    if (fileSize) {
      m_diskReader.push(drt.release());
    } else {
      m_lc.log(cta::log::WARNING, "Skipped disk read task creation for zero-length file.");
    }
  }
  LogContext::ScopedParam sp(m_lc, Param("numberOfFiles", jobs.size()));
  m_lc.log(cta::log::INFO, "Finished creating tasks for migrating");
}

//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
void MigrationTaskInjector::waitThreads() const {
  m_thread.wait();
}

//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
void MigrationTaskInjector::startThreads() {
  m_thread.start();
}

//------------------------------------------------------------------------------
//requestInjection
//------------------------------------------------------------------------------
void MigrationTaskInjector::requestInjection(bool lastCall) {
  cta::threading::MutexLocker ml(m_producerProtection);
  m_queue.push(Request(m_maxFiles, m_maxBytes, lastCall));
}

//------------------------------------------------------------------------------
//synchronousInjection
//------------------------------------------------------------------------------
bool MigrationTaskInjector::synchronousInjection(bool& noFilesToMigrate) {
  std::list<std::unique_ptr<cta::ArchiveJob>> jobs;
  noFilesToMigrate = false;
  try {
    //First popping of files, we multiply the number of popped files / bytes by 2 to avoid multiple mounts on Repack
    //(it is applied to ArchiveForUser and ArchiveForRepack batches)
    m_lc.log(cta::log::DEBUG, "Before m_archiveMount.getNextJobBatch()");
    jobs = m_archiveMount.getNextJobBatch(2 * m_maxFiles, 2 * m_maxBytes, m_lc);
    m_lc.log(cta::log::DEBUG, "After m_archiveMount.getNextJobBatch()");
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer scoped(m_lc);
    scoped.add("transactionId", m_archiveMount.getMountTransactionId())
      .add("byteSizeThreshold", m_maxBytes)
      .add("maxFiles", m_maxFiles)
      .add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(cta::log::ERR, "Failed to getFilesToMigrate");
    return false;
  }
  cta::log::ScopedParamContainer scoped(m_lc);
  scoped.add("byteSizeThreshold", m_maxBytes).add("maxFiles", m_maxFiles);
  if (jobs.empty()) {
    noFilesToMigrate = true;
    m_lc.log(cta::log::WARNING, "No files to migrate: empty mount");
    return false;
  } else {
    m_firstFseqToWrite = jobs.front()->tapeFile.fSeq;
    injectBulkMigrations(jobs);
    return true;
  }
}

//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------
void MigrationTaskInjector::finish() {
  cta::threading::MutexLocker ml(m_producerProtection);
  m_queue.push(Request());
}

//------------------------------------------------------------------------------
//signalEndDataMovement
//------------------------------------------------------------------------------
void MigrationTaskInjector::signalEndDataMovement() {
  //first send the end signal to the threads
  m_tapeWriter.finish();
  m_diskReader.finish();
  m_memManager.finish();
}

double MigrationTaskInjector::calculateFillRatio(const uint64_t fetched, const uint64_t requested) {
  if (requested == 0) {
    return 0.0;
  }

  const double ratio = static_cast<double>(fetched) / static_cast<double>(requested);

  return std::min(1.0, ratio);
}

bool MigrationTaskInjector::shouldDismountForUnderfill(const uint64_t filesFetched,
                                                       const uint64_t bytesFetched,
                                                       const Request& request) {
  /*
   * The queue-end sentinel is not a backend response and must not affect
   * underfill statistics.
   */
  if (request.end) {
    return false;
  }

  /*
   * A request without either limit cannot produce a meaningful fill ratio.
   * Do not turn such a malformed request into a dismount decision.
   */
  if (request.filesRequested == 0 && request.bytesRequested == 0) {
    cta::log::ScopedParamContainer params(m_lc);

    params.add("filesFetched", filesFetched).add("bytesFetched", bytesFetched);

    params.log(cta::log::WARNING,
               "Migration request has both file and byte limits set to zero. "
               "Skipping underfill evaluation.");

    return false;
  }

  const double fileFillRatio = calculateFillRatio(filesFetched, request.filesRequested);

  const double byteFillRatio = calculateFillRatio(bytesFetched, request.bytesRequested);

  /*
   * Either the file limit or the byte limit can constrain the batch.
   *
   * Using the maximum correctly considers:
   * - a small number of large files; and
   * - a large number of small files.
   */
  const double effectiveFillRatio = std::max(fileFillRatio, byteFillRatio);

  /*
   * No underfill period is active.
   */
  if (!m_underfillTimer.has_value()) {
    if (effectiveFillRatio >= m_underfillStartThreshold / 100.) {
      cta::log::ScopedParamContainer params(m_lc);

      params.add("filesRequested", request.filesRequested)
        .add("bytesRequested", request.bytesRequested)
        .add("filesFetched", filesFetched)
        .add("bytesFetched", bytesFetched)
        .add("fileFillPercentage", fileFillRatio * 100.0)
        .add("byteFillPercentage", byteFillRatio * 100.0)
        .add("effectiveFillPercentage", effectiveFillRatio * 100.0)
        .add("underfillStartPercentage", m_underfillStartThreshold);

      params.log(cta::log::DEBUG, "Migration response did not start an underfill observation period.");

      return false;
    }

    /*
     * Constructing the timer sets its reference point to the current
     * monotonic time.
     */
    m_underfillTimer.emplace();
    m_underfillSamples = 1;

    cta::log::ScopedParamContainer params(m_lc);

    params.add("filesRequested", request.filesRequested)
      .add("bytesRequested", request.bytesRequested)
      .add("filesFetched", filesFetched)
      .add("bytesFetched", bytesFetched)
      .add("fileFillPercentage", fileFillRatio * 100.0)
      .add("byteFillPercentage", byteFillRatio * 100.0)
      .add("effectiveFillPercentage", effectiveFillRatio * 100.0)
      .add("underfillStartPercentage", m_underfillStartThreshold)
      .add("underfillSamples", m_underfillSamples);

    params.log(cta::log::INFO, "Started migration-response underfill observation period.");

    return false;
  }

  /*
   * An underfill period is active. A response at or above the recovery
   * threshold ends the observation period.
   */
  if (effectiveFillRatio >= m_underfillRecoveryThreshold / 100.) {
    const double underfillDurationSeconds = m_underfillTimer->secs();

    cta::log::ScopedParamContainer params(m_lc);

    params.add("filesRequested", request.filesRequested)
      .add("bytesRequested", request.bytesRequested)
      .add("filesFetched", filesFetched)
      .add("bytesFetched", bytesFetched)
      .add("fileFillPercentage", fileFillRatio * 100.0)
      .add("byteFillPercentage", byteFillRatio * 100.0)
      .add("effectiveFillPercentage", effectiveFillRatio * 100.0)
      .add("underfillRecoveryPercentage", m_underfillRecoveryThreshold)
      .add("underfillSamples", m_underfillSamples)
      .add("underfillDurationSeconds", underfillDurationSeconds);

    params.log(cta::log::INFO, "Migration responses recovered from the active underfill period.");

    /*
     * optional::reset() destroys the Timer and marks underfill as inactive.
     * This is intentionally not m_underfillTimer->reset(), which would merely
     * restart an active timer.
     */
    m_underfillTimer.reset();
    m_underfillSamples = 0;

    return false;
  }

  /*
   * The response did not reach the recovery threshold.
   *
   * This includes responses:
   * - below the start threshold; and
   * - in the hysteresis range between the start and recovery thresholds.
   */
  if (m_underfillSamples < std::numeric_limits<uint64_t>::max()) {
    ++m_underfillSamples;
  }

  const double underfillDurationSeconds = m_underfillTimer->secs();

  const bool durationExpired = underfillDurationSeconds >= m_underfillWatchPeriodSecs;

  const bool enoughSamples = m_underfillSamples >= m_underfillMinSamples;

  cta::log::ScopedParamContainer params(m_lc);

  params.add("filesRequested", request.filesRequested)
    .add("bytesRequested", request.bytesRequested)
    .add("filesFetched", filesFetched)
    .add("bytesFetched", bytesFetched)
    .add("fileFillPercentage", fileFillRatio * 100.0)
    .add("byteFillPercentage", byteFillRatio * 100.0)
    .add("effectiveFillPercentage", effectiveFillRatio * 100.0)
    .add("underfillRecoveryPercentage", m_underfillRecoveryThreshold)
    .add("underfillSamples", m_underfillSamples)
    .add("minimumUnderfillSamples", m_underfillMinSamples)
    .add("underfillDurationSeconds", underfillDurationSeconds)
    .add("underfillWatchPeriodSeconds", m_underfillWatchPeriodSecs);

  if (durationExpired && enoughSamples) {
    params.log(cta::log::INFO,
               "Migration responses remained underfilled for the configured duration "
               "and minimum sample count. Triggering the end of the tape session.");

    return true;
  }

  params.log(cta::log::DEBUG,
             "Migration response remains underfilled, but the dismount conditions "
             "have not both been reached.");

  return false;
}

//------------------------------------------------------------------------------
//WorkerThread::run
//------------------------------------------------------------------------------
void MigrationTaskInjector::WorkerThread::run() {
  m_parent.m_lc.push(Param("thread", "MigrationTaskInjector"));
  m_parent.m_lc.log(cta::log::INFO, "Starting MigrationTaskInjector thread");
  try {
    while (true) {
      if (m_parent.m_errorFlag) {
        throw cta::tape::daemon::ErrorFlag();
      }
      Request req = m_parent.m_queue.pop();
      m_parent.m_lc.log(cta::log::DEBUG,
                        "MigrationTaskInjector::WorkerThread::run(): Trying to get jobs from archive mount");
      auto jobs = m_parent.m_archiveMount.getNextJobBatch(req.filesRequested, req.bytesRequested, m_parent.m_lc);
      uint64_t filesFetched = jobs.size();
      uint64_t bytesFetched = 0;
      for (auto& j : jobs) {
        bytesFetched += j->archiveFile.fileSize;
      }
      if (jobs.empty()) {
        m_parent.m_lc.log(cta::log::DEBUG, "MigrationTaskInjector::WorkerThread::run(): No jobs were found");
        if (req.lastCall) {
          m_parent.m_lc.log(cta::log::INFO, "No more files to migrate: triggering the end of session.");
          m_parent.signalEndDataMovement();
          break;
        } else {
          m_parent.m_lc.log(cta::log::INFO,
                            "In MigrationTaskInjector::WorkerThread::run(): got empty list, but not last call");
        }
      } else {
        m_parent.m_lc.log(cta::log::DEBUG, "MigrationTaskInjector::WorkerThread::run(): injectBulkMigrations");
        // Inject the tasks
        m_parent.injectBulkMigrations(jobs);
        // Decide on continuation
        //if (files < req.filesRequested / 2 && bytes < req.bytesRequested) {
        //  // The client starts to dribble files at a low rate. Better finish
        //  // the session now, so we get a clean batch on a later mount.
        //  cta::log::ScopedParamContainer params(m_parent.m_lc);
        //  params.add("filesRequested", req.filesRequested)
        //    .add("bytesRequested", req.bytesRequested)
        //    .add("filesReceived", files)
        //    .add("bytesReceived", bytes);
        //  m_parent.m_lc.log(cta::log::INFO,
        //                    "Got less than half the requested work to do: triggering the end of session.");
        //  m_parent.signalEndDataMovement();
        //  break;
      }
      /*
     * Evaluate all non-final backend responses, including empty responses.
     *
     * An empty non-final response contributes:
     *
     *   filesFetched = 0
     *   bytesFetched = 0
     *   effective fill = 0%
     */
      if (m_parent.shouldDismountForUnderfill(filesFetched, bytesFetched, req)) {
        m_parent.signalEndDataMovement();
        break;
      }

    }  //end of while(1)
  } catch (const cta::tape::daemon::ErrorFlag&) {
    //we end up there because a task screw up somewhere
    m_parent.m_lc.log(cta::log::INFO,
                      "In MigrationTaskInjector::WorkerThread::run(): a task failed, "
                      "indicating finish of run");

    m_parent.signalEndDataMovement();
  } catch (const cta::exception::Exception& ex) {
    //we end up there because we could not talk to the client

    cta::log::ScopedParamContainer container(m_parent.m_lc);
    container.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_parent.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
    m_parent.m_lc.log(cta::log::ERR,
                      "In MigrationTaskInjector::WorkerThread::run(): "
                      "could not retrieve a list of file to migrate, indicating finish of run");

    m_parent.signalEndDataMovement();
  }
  //-------------
  m_parent.m_lc.log(cta::log::INFO, "Finishing MigrationTaskInjector thread");
  /* We want to finish at the first lastCall we encounter.
     * But even after sending finish() to m_diskWriter and to m_tapeReader,
     * m_diskWriter might still want some more task (the threshold could be crossed),
     * so we discard everything that might still be in the queue
     */
  bool stillReading = true;
  while (stillReading) {
    Request req = m_parent.m_queue.pop();
    if (req.end) {
      stillReading = false;
    }
    LogContext::ScopedParam sp(m_parent.m_lc, Param("lastCall", req.lastCall));
    m_parent.m_lc.log(cta::log::INFO, "In MigrationTaskInjector::WorkerThread::run(): popping extra request");
  }
}

uint64_t MigrationTaskInjector::firstFseqToWrite() const {
  return m_firstFseqToWrite;
}

}  // namespace cta::tape::daemon
