/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeWriteSingleThread.hpp"

#include "MigrationTaskInjector.hpp"
#include "common/dataStructures/DriveDownReason.hpp"

#include <optional>

//------------------------------------------------------------------------------
// Constructor for TapeWriteSingleThread
//------------------------------------------------------------------------------
cta::tape::daemon::TapeWriteSingleThread::TapeWriteSingleThread(cta::tape::drive::DriveInterface& drive,
                                                                cta::mediachanger::MediaChangerFacade& mediaChanger,
                                                                TapeSessionTracker& tracker,
                                                                const VolumeInfo& volInfo,
                                                                const cta::log::LogContext& logContext,
                                                                MigrationReportPacker& reportPacker,
                                                                uint64_t filesBeforeFlush,
                                                                uint64_t bytesBeforeFlush,
                                                                const bool useLbp,
                                                                const bool useEncryption,
                                                                const std::string& externalEncryptionKeyScript,
                                                                const cta::ArchiveMount& archiveMount,
                                                                const uint64_t tapeLoadTimeout,
                                                                cta::catalogue::Catalogue& catalogue)
    : TapeSingleThreadInterface<TapeWriteTask>(drive,
                                               mediaChanger,
                                               tracker,
                                               volInfo,
                                               logContext,
                                               useEncryption,
                                               externalEncryptionKeyScript,
                                               tapeLoadTimeout),
      m_filesBeforeFlush(filesBeforeFlush),
      m_bytesBeforeFlush(bytesBeforeFlush),
      m_reportPacker(reportPacker),
      m_useLbp(useLbp),
      m_archiveMount(archiveMount),
      m_catalogue(catalogue) {}

//------------------------------------------------------------------------------
//TapeCleaning::~TapeCleaning()
//------------------------------------------------------------------------------
cta::tape::daemon::TapeWriteSingleThread::TapeCleaning::~TapeCleaning() {
  m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::CleaningUp,
                                          std::nullopt,
                                          m_this.m_logContext);

  // Tell everyone to wrap up the session
  // We now acknowledge to the task injector that read reached the end. There
  // will hence be no more requests for more.
  m_this.m_taskInjector->finish();
  //then we log/notify
  m_this.m_logContext.log(cta::log::DEBUG,
                          "Starting write session cleanup. Signalled end of session to task injector.");
  m_this.m_tracker.addDiskTransferStats({.waitReportingTime = m_timer.secs(cta::utils::Timer::resetCounter)});

  // Disable encryption (or at least try)
  try {
    if (m_this.m_encryptionControl.disable(m_this.m_drive)) {
      m_this.m_logContext.log(cta::log::INFO, "Turned encryption off before unmounting");
    }
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer scoped(m_this.m_logContext);
    scoped.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_this.m_logContext.log(cta::log::ERR, "Failed to turn off encryption before unmounting");
  }
  m_this.m_tracker.addTapeCleanupStats({.encryptionControlTime = m_timer.secs(cta::utils::Timer::resetCounter)});

  // Log (safely, exception-wise) the tape alerts (if any) at the end of the session
  try {
    m_this.logTapeAlerts();
  } catch (...) {}
  // Log (safely, exception-wise) the tape SCSI metrics at the end of the session
  try {
    m_this.logSCSIMetrics();
  } catch (...) {}

  // Log safely errors at the end of the session
  // This out-of-try-catch variables allows us to record the stage of the
  // process we're in, and to count the error if it occurs.
  // We will not record errors for an empty string. This will allow us to
  // prevent counting where error happened upstream.
  TapeSessionError currentErrorToCount = TapeSessionError::TapeUnload;
  try {
    // Do the final cleanup
    // First check that a tape is actually present in the drive. We can get here
    // after failing to mount (library error) in which case there is nothing to
    // do (and trying to unmount will only lead to a failure.)
    // We give time to the drive for settling after a mount which might have
    // just happened. If we time out, then we will simply find no tape in the
    // drive, which is a fine situation (so timeout exceptions are discarded).
    // Other exception, where we failed to access the drive somehow are at passed
    // through.
    const uint32_t waitMediaInDriveTimeout = m_this.m_tapeLoadTimeout;
    try {
      m_this.m_drive.waitUntilReady(waitMediaInDriveTimeout);
    } catch (cta::exception::TimeOut&) {}
    if (!m_this.m_drive.hasTapeInPlace()) {
      m_this.m_logContext.log(cta::log::INFO, "TapeWriteSingleThread: No tape to unload");
      m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Up,
                                              std::nullopt,
                                              m_this.m_logContext);
      m_this.m_tracker.reportState(cta::tape::session::SessionState::ShuttingDown,
                                   cta::tape::session::SessionType::Archive);
      return;
    }

    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unloading,
                                            std::nullopt,
                                            m_this.m_logContext);
    m_this.m_drive.unloadTape();
    m_this.m_logContext.log(cta::log::INFO, "TapeWriteSingleThread: Tape unloaded");
    m_this.m_tracker.addTapeCleanupStats({.unloadTime = m_timer.secs(cta::utils::Timer::resetCounter)});

    // And return the tape to the library
    currentErrorToCount = TapeSessionError::TapeDismount;
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting,
                                            std::nullopt,
                                            m_this.m_logContext);
    m_this.m_tracker.reportState(cta::tape::session::SessionState::Unmounting,
                                 cta::tape::session::SessionType::Archive);
    const auto librarySlot = cta::mediachanger::LibrarySlotParser::parse(m_this.m_drive.info.rawLibrarySlot);
    m_this.m_mediaChanger.dismountTape(m_this.m_volInfo.vid, librarySlot);
    m_this.m_drive.disableLogicalBlockProtection();
    m_this.m_tracker.addTapeCleanupStats({.unmountTime = m_timer.secs(cta::utils::Timer::resetCounter)});
    m_this.m_logContext.log(cta::log::INFO, "TapeWriteSingleThread : tape unmounted");
    // We know we are the last thread, just report the drive Up
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Up,
                                            std::nullopt,
                                            m_this.m_logContext);
    m_this.m_tracker.reportState(cta::tape::session::SessionState::ShuttingDown,
                                 cta::tape::session::SessionType::Archive);
    m_this.m_tracker.addDiskTransferStats({.waitReportingTime = m_timer.secs(cta::utils::Timer::resetCounter)});
  } catch (const cta::exception::Exception& ex) {
    // Notify something failed during the cleaning
    m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
    const int logLevel = cta::common::dataStructures::driveDownReasonSeverity(
      cta::common::dataStructures::DriveDownReason::TapeCleanupFailed);
    const std::string errorMsg =
      "Exception in TapeWriteSingleThread-TapeCleaning when unmounting/unloading the tape. Putting the drive down.";
    std::optional<std::string> reason = cta::common::dataStructures::formatDriveDownReason(
      cta::common::dataStructures::DriveDownReason::TapeCleanupFailed);
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Down,
                                            reason,
                                            m_this.m_logContext);
    m_this.m_tracker.reportState(cta::tape::session::SessionState::Fatal, cta::tape::session::SessionType::Archive);
    cta::log::ScopedParamContainer scoped(m_this.m_logContext);
    scoped.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_this.m_logContext.log(logLevel, errorMsg);

    // As we do not throw exceptions from here, the tracker signalling has
    // to occur from here.
    try {
      m_this.m_tracker.incrementError(currentErrorToCount);
    } catch (...) {}
  } catch (...) {
    // Notify something failed during the cleaning
    m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
    const int logLevel = cta::common::dataStructures::driveDownReasonSeverity(
      cta::common::dataStructures::DriveDownReason::TapeCleanupFailed);
    const std::string errorMsg =
      "Non-CTA exception in TapeWriteSingleThread-TapeCleaning when unmounting the tape. Putting the drive down.";
    std::optional<std::string> reason = cta::common::dataStructures::formatDriveDownReason(
      cta::common::dataStructures::DriveDownReason::TapeCleanupFailed);
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Down,
                                            reason,
                                            m_this.m_logContext);
    m_this.m_tracker.reportState(cta::tape::session::SessionState::Fatal, cta::tape::session::SessionType::Archive);
    m_this.m_logContext.log(logLevel, errorMsg);
    try {
      m_this.m_tracker.incrementError(currentErrorToCount);
    } catch (...) {}
  }
}

//------------------------------------------------------------------------------
//setlastFseq
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeWriteSingleThread::setlastFseq(uint64_t lastFseq) {
  m_lastFseq = lastFseq;
}

//------------------------------------------------------------------------------
//openWriteSession
//------------------------------------------------------------------------------
std::unique_ptr<cta::tape::tapeFile::WriteSession> cta::tape::daemon::TapeWriteSingleThread::openWriteSession() {
  cta::log::ScopedParamContainer params(m_logContext);
  params.add("lastFseq", m_lastFseq).add("compression", m_compress).add("useLbp", m_useLbp);

  try {
    auto writeSession =
      std::make_unique<cta::tape::tapeFile::WriteSession>(m_drive, m_volInfo, m_lastFseq, m_compress, m_useLbp);

    return writeSession;
  } catch (cta::exception::Exception& e) {
    // TODO: log and unroll the session
    // TODO: add an unroll mode to the tape read task. (Similar to exec, but pushing blocks marked in error)
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add(cta::semconv::log::exceptionMessage, e.getMessageValue());
    m_logContext.log(cta::log::ERR, "Failed to start tape write session");
    throw;
  }
}

//------------------------------------------------------------------------------
//tapeFlush
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeWriteSingleThread::tapeFlush(const std::string& message,
                                                         uint64_t bytes,
                                                         uint64_t files,
                                                         cta::utils::Timer& timer) {
  m_drive.flush();
  double flushTime = timer.secs(cta::utils::Timer::resetCounter);
  cta::log::ScopedParamContainer params(m_logContext);
  params.add("files", files).add("bytes", bytes).add("flushTime", flushTime);
  m_logContext.log(cta::log::INFO, message);
  m_stats.flushTime += flushTime;

  m_reportPacker.reportFlush(m_drive.getCompression(), m_logContext);
  m_drive.clearCompressionStats();
}

//------------------------------------------------------------------------
//   logAndCheckTapeAlertsForWrite
//------------------------------------------------------------------------------
bool cta::tape::daemon::TapeWriteSingleThread::logAndCheckTapeAlertsForWrite() {
  std::vector<uint16_t> tapeAlertCodes = m_drive.getTapeAlertCodes();
  if (tapeAlertCodes.empty()) {
    return false;
  }
  size_t alertNumber = 0;
  // Log tape alerts in the logs.
  std::vector<std::string> tapeAlerts = m_drive.getTapeAlerts(tapeAlertCodes);
  for (const auto& ta : tapeAlerts) {
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("tapeAlert", ta).add("tapeAlertNumber", alertNumber++).add("tapeAlertCount", tapeAlerts.size());
    m_logContext.log(cta::log::WARNING, "Tape alert detected");
  }
  // Add tape alerts in the tape log parameters
  for (const auto tapeAlertCode : tapeAlertCodes) {
    countTapeAlert(tapeAlertCode);
  }
  return (m_drive.tapeAlertsCriticalForWrite(tapeAlertCodes));
}

//------------------------------------------------------------------------------
//   isTapeWritable
//-----------------------------------------------------------------------------
void cta::tape::daemon::TapeWriteSingleThread::isTapeWritable() const {
  // check that drive is not write protected
  if (m_drive.isWriteProtected()) {
    cta::exception::Exception ex;
    ex.getMessage() << "End session with error. Drive is write protected. Aborting labelling...";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// TapeWriteSingleThread::run()
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeWriteSingleThread::run() {
  cta::log::ScopedParamContainer threadGlobalParams(m_logContext);
  threadGlobalParams.add("thread", "TapeWrite");
  cta::utils::Timer timer, totalTimer;
  // This out-of-try-catch variables allows us to record the stage of the
  // process we're in, and to count the error if it occurs.
  // We will not record errors for an empty string. This will allow us to
  // prevent counting where error happened upstream.
  std::optional<TapeSessionError> currentErrorToCount = TapeSessionError::TapeMountForWrite;
  std::unique_ptr<TapeWriteTask> task;

  try {
    // Pair of brackets to create an artificial scope for the tape cleaning
    {
      // Log and notify
      m_logContext.log(cta::log::INFO, "Starting tape write thread");

      // The tape will be loaded
      // it has to be unloaded, unmounted at all cost -> RAII
      // will also take care of the TapeSessionReporter
      TapeCleaning cleaner(*this, timer);
      // Before anything, the tape should be mounted
      m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Mounting, std::nullopt, m_logContext);
      m_tracker.reportState(cta::tape::session::SessionState::Mounting, cta::tape::session::SessionType::Archive);
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("mediaType", m_archiveMount.getMediaType());
      params.add("logicalLibrary", m_drive.info.logicalLibrary);
      params.add("mountType", toCamelCaseString(m_volInfo.mountType));
      params.add("vendor", m_archiveMount.getVendor());
      params.add("capacityInBytes", m_archiveMount.getCapacityInBytes());
      m_logContext.log(cta::log::INFO, "Tape session started for write");
      measureSetupTime(&TapeSetupStats::initialMountTime, [&] { mountTapeReadWrite(); });
      currentErrorToCount = TapeSessionError::TapeLoad;
      measureSetupTime(&TapeSetupStats::tapeLoadTime, [&] { waitForDrive(); });
      const double tapeLoadTime = m_tracker.stats().setup.tapeLoadTime;
      currentErrorToCount = TapeSessionError::CheckingTapeAlert;
      if (logAndCheckTapeAlertsForWrite()) {
        throw cta::exception::Exception("Aborting write session in"
                                        " presence of critical tape alerts");
      }

      currentErrorToCount = TapeSessionError::TapeNotWriteable;
      isTapeWritable();

      m_tracker.addTapeSetupStats({.mountTime = timer.secs(cta::utils::Timer::resetCounter)});
      {
        cta::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("mountTime", m_tracker.stats().setup.mountTime);
        scoped.add("tapeLoadTime", tapeLoadTime);
        m_logContext.log(cta::log::INFO, "Tape mounted and drive ready");
      }
      m_archiveMount.setTapeMounted(m_logContext);
      try {
        currentErrorToCount = TapeSessionError::TapeEncryptionEnable;
        // We want those scoped params to last for the whole mount.
        // This will allow each written file to be logged with its encryption
        // status:
        cta::log::ScopedParamContainer encryptionLogParams(m_logContext);
        {
          auto encryptionStatus = m_encryptionControl.enable(m_drive, m_volInfo, m_catalogue, true);
          if (encryptionStatus.on) {
            encryptionLogParams.add("encryption", "on")
              .add("encryptionKeyName", encryptionStatus.keyName)
              .add("scriptPath", m_encryptionControl.getScriptPath())
              .add("stdout", encryptionStatus.stdout);
            m_logContext.log(cta::log::INFO, "Drive encryption enabled for this mount");
          } else {
            encryptionLogParams.add("encryption", "off");
            m_logContext.log(cta::log::INFO, "Drive encryption not enabled for this mount");
          }
        }
        m_tracker.addTapeSetupStats({.encryptionControlTime = timer.secs(cta::utils::Timer::resetCounter)});
      } catch (cta::exception::Exception& ex) {
        cta::log::ScopedParamContainer exceptionParams(m_logContext);
        exceptionParams.add(cta::semconv::log::exceptionMessage, ex.getMessage().str());
        m_logContext.log(cta::log::ERR, "Drive encryption could not be enabled for this mount.");
        throw;
      }
      // Then we have to initialize the tape write session
      currentErrorToCount = TapeSessionError::TapePositionForWrite;
      auto writeSession = openWriteSession();
      m_tracker.addTapeSetupStats({.positionTime = timer.secs(cta::utils::Timer::resetCounter)});
      //and then report
      {
        cta::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("positionTime", m_tracker.stats().setup.positionTime);
        scoped.add("useLbp", m_useLbp);
        scoped.add("detectedLbp", writeSession->isTapeWithLbp());

        if (!writeSession->isTapeWithLbp() && m_useLbp) {
          m_logContext.log(cta::log::INFO,
                           "Taped started with LBP support but "
                           "the tape without LBP label mounted");
        }
        switch (m_drive.getLbpToUse()) {
          case drive::lbpToUse::crc32cReadWrite:
            m_logContext.log(cta::log::INFO,
                             "Write session initialised with LBP"
                             " crc32c in ReadWrite mode, tape VID checked and drive positioned"
                             " for writing");
            break;
          case drive::lbpToUse::disabled:
            m_logContext.log(cta::log::INFO,
                             "Write session initialised without LBP"
                             ", tape VID checked and drive positioned for writing");
            break;
          default:
            m_logContext.log(cta::log::ERR,
                             "Write session initialised with "
                             "unsupported LBP method, tape VID checked and drive positioned"
                             " for writing");
        }
      }

      m_tracker.addDiskTransferStats({.waitReportingTime = timer.secs(cta::utils::Timer::resetCounter)});

      uint64_t bytes = 0;
      uint64_t files = 0;
      // Tasks handle their error logging themselves.
      currentErrorToCount.reset();
      m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Transferring,
                                       std::nullopt,
                                       m_logContext);
      m_tracker.reportState(cta::tape::session::SessionState::Running, cta::tape::session::SessionType::Archive);
      while (true) {
        //get a task
        task.reset(m_tasks.pop());
        m_stats.waitInstructionsTime += timer.secs(cta::utils::Timer::resetCounter);
        // If we reached the end
        if (nullptr == task) {
          //we flush without asking
          tapeFlush("No more data to write on tape, unconditional flushing to the client", bytes, files, timer);
          m_stats.flushTime += timer.secs(cta::utils::Timer::resetCounter);
          cta::log::LogContext::ScopedParam sp0(m_logContext, cta::log::Param("tapeThreadDuration", totalTimer.secs()));
          m_logContext.log(cta::log::DEBUG, "writing data to tape has finished");
          break;
        }
        task->execute(*writeSession, m_reportPacker, m_tracker, m_logContext, timer);
        // Add the tasks counts to the session's
        m_stats.add(task->getTaskStats());
        // Publish the current statistics to the session tracker.
        m_tracker.updateTapeTransferStats(m_stats);
        // Increase local flush counters (session counters are incremented by
        // the task)
        files++;
        bytes += task->fileSize();
        //if one flush counter is above a threshold, then we flush
        if (files >= m_filesBeforeFlush || bytes >= m_bytesBeforeFlush) {
          currentErrorToCount = TapeSessionError::TapeFlush;
          tapeFlush("Normal flush because thresholds was reached", bytes, files, timer);
          files = 0;
          bytes = 0;
          currentErrorToCount.reset();
        }
      }  //end of while(true))
    }

    // The session completed successfully, and the cleaner (unmount) executed
    // at the end of the previous block. Log the results.
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("status", m_tracker.errorHappened() ? "error" : "success");
    m_totalTime = totalTimer.secs();
    m_tracker.setTotalTime(m_totalTime);
    m_tracker.setDiskDeliveryTime(m_totalTime);
    logWithStats(cta::log::INFO, "Tape thread complete", params);
    // Report one last time the stats, after unloading/unmounting.
    m_tracker.updateTapeTransferStats(m_stats);
    //end of session + log
    m_reportPacker.reportEndOfSession(m_logContext);
  } catch (const cta::exception::Exception& e) {
    //we end there because write session could not be opened
    //or because a task failed or because flush failed

    // First off, indicate the problem to the task injector so it does not inject
    // more work in the pipeline
    // If the problem did not originate here, we just re-flag the error, and
    // this has no effect, but if we had a problem with a non-file operation
    // like mounting the tape, then we have to signal the problem to the disk
    // side and the task injector, which will trigger the end of session.
    m_taskInjector->setErrorFlag();
    // Publish transfer statistics; the RAII cleaner recorded cleanup timings independently.
    m_tracker.updateTapeTransferStats(m_stats);
#ifdef CTA_PGSCHED
    // report last batch of files which were written but not flushed
    // (no file marks on tape) as failure (last file written to tape when ENOSPC
    // (end of space) was thrown will be included here)
    m_reportPacker.reportLastBatchError(e, m_logContext);
#endif

    // If we reached the end of tape, this is not an error (ENOSPC)
    bool isTapeFull = false;
    try {
      // If it's not the error we're looking for, we will go about our business
      // in the catch section. dynamic cast will throw, and we'll do ourselves
      // if the error code is not the one we want.
      if (const auto& en = dynamic_cast<const cta::exception::Errnum&>(e); en.errorNumber() != ENOSPC) {
        throw 0;
      } else {
        isTapeFull = true;
      }
      // This is indeed the end of the tape. Not an error.
      m_tracker.setErrorCount(TapeSessionError::TapeFilledUp, 1);
      m_reportPacker.reportTapeFull(m_logContext);
    } catch (...) {
      // The error is not an ENOSPC, so it is, indeed, an error.
      // If we got here with a new error, currentErrorToCount will be non-empty,
      // and we will pass the typed error to the session tracker.
      if (currentErrorToCount) {
        m_tracker.incrementError(*currentErrorToCount);
      }
    }
#ifdef CTA_PGSCHED
    // If isTapeFull is true, it is not possible to run flushTape() (no space for the file marks)
    // in this case last job has already been reported as completed, in other case as failed
    // if TapeWriteTasked crashed before the last job was reported, we try to report it here
    std::list<std::string> jobIDsList;
    try {
      if (nullptr != task && task->hasArchiveJob()) {
        jobIDsList.emplace_back(task->getArchiveJob().getJobID());
      }
    } catch (cta::exception::Exception& ex) {
      cta::log::ScopedParamContainer exceptionParams(m_logContext);
      exceptionParams.add(cta::semconv::log::exceptionMessage, ex.getMessage().str());
      m_logContext.log(
        cta::log::ERR,
        "TapeWriteSingleThread::run(): job ID could not be retrieved for the last task of the crashed session.");
    }
    m_logContext.log(cta::log::DEBUG, "TapeWriteSingleThread::run(): CheckNr3: After jobIDsList assembly.");
#endif
    // empty all the remaining tasks waiting in the queue and circulate mem blocks
    while (true) {
      std::unique_ptr<TapeWriteTask> remaining_task(m_tasks.pop());
      if (remaining_task == nullptr) {
        break;
      }
#ifdef CTA_PGSCHED
      // prepare job IDs for re-queueing
      if (remaining_task->hasArchiveJob()) {
        jobIDsList.emplace_back(remaining_task->getArchiveJob().getJobID());
      }
#endif
      remaining_task->circulateMemBlocks();
    }
#ifdef CTA_PGSCHED
    // requeue the unprocessed tasks
    requeueUnprocessedTasks(jobIDsList, m_logContext);
#endif
    // Prepare the standard error codes for the session
    std::string errorMessage(e.getMessageValue());
    int logLevel = cta::log::ERR;
    // Override if we got en ENOSPC error (end of tape)
    if (isTapeFull) {
      errorMessage = "End of migration due to tape full";
      logLevel = cta::log::INFO;
    }
    // prepare logging params
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("status", "error").add(cta::semconv::log::exceptionMessage, errorMessage);
    m_totalTime = totalTimer.secs();
    m_tracker.setTotalTime(m_totalTime);
    logWithStats(logLevel, "Tape thread complete for writing", params);
    m_reportPacker.reportEndOfSessionWithErrors(errorMessage, isTapeFull, m_logContext);
  }
}

//------------------------------------------------------------------------------
//logWithStats
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeWriteSingleThread::logWithStats(int level,
                                                            const std::string& msg,
                                                            cta::log::ScopedParamContainer& params) {
  const auto sessionStats = m_tracker.stats();
  params.add("type", "write")
    .add("tapeVid", m_volInfo.vid)
    .add("mountTime", sessionStats.setup.mountTime)
    .add("initialMountTime", sessionStats.setup.initialMountTime)
    .add("tapeLoadTime", sessionStats.setup.tapeLoadTime)
    .add("positionTime", sessionStats.setup.positionTime + m_stats.positionTime)
    .add("waitInstructionsTime", m_stats.waitInstructionsTime)
    .add("checksumingTime", m_stats.checksumingTime)
    .add("readWriteTime", m_stats.readWriteTime)
    .add("waitDataTime", m_stats.waitDataTime)
    .add("waitReportingTime", sessionStats.disk.waitReportingTime)
    .add("flushTime", m_stats.flushTime)
    .add("unloadTime", sessionStats.cleanup.unloadTime)
    .add("unmountTime", sessionStats.cleanup.unmountTime)
    .add("encryptionControlTime", sessionStats.setup.encryptionControlTime + sessionStats.cleanup.encryptionControlTime)
    .add("transferTime", m_stats.transferTime())
    .add("totalTime", m_totalTime)
    .add("dataVolume", m_stats.dataVolume)
    .add("headerVolume", m_stats.headerVolume)
    .add("files", m_stats.filesCount)
    .add("payloadTransferSpeedMBps", m_totalTime ? 1.0 * m_stats.dataVolume / 1000 / 1000 / m_totalTime : 0.0)
    .add("driveTransferSpeedMBps",
         m_totalTime ? 1.0 * (m_stats.dataVolume + m_stats.headerVolume) / 1000 / 1000 / m_totalTime : 0.0);
  m_logContext.log(level, msg);
}

//------------------------------------------------------------------------------
//   requeueFailedTask - for PGCHED
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeWriteSingleThread::requeueUnprocessedTasks(const std::list<std::string>& jobIDsList,
                                                                       cta::log::LogContext& lc) const {
  uint64_t njobs = m_archiveMount.requeueJobBatch(jobIDsList, lc);
  cta::log::ScopedParamContainer requeueparam(lc);
  requeueparam.add("requeuedTaskQueueJobs", njobs);

  if (njobs != jobIDsList.size()) {
    // handle the case of failed bunch update of the jobs !
    std::string jobIDsString;
    for (const auto& piece : jobIDsList) {
      jobIDsString += piece;
    }
    lc.log(cta::log::ERR,
           std::string("In TapeWriteSingleThread::requeueUnprocessedTasks(): Did not requeue all task jobs of "
                       "the remaining queue, job IDs attempting to update were: ")
             + jobIDsString);
  } else {
    lc.log(cta::log::INFO, std::string("In TapeWriteSingleThread::requeueUnprocessedTasks(): success."));
  }
}

//------------------------------------------------------------------------------
//logSCSIMetrics
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeWriteSingleThread::logSCSIMetrics() {
  try {
    // mount general statistics
    cta::log::ScopedParamContainer scopedContainer(m_logContext);
    appendDriveAndTapeInfoToScopedParams(scopedContainer);
    // get mount general stats
    std::map<std::string, uint64_t> scsi_write_metrics_hash = m_drive.getTapeWriteErrors();
    appendMetricsToScopedParams(scopedContainer, scsi_write_metrics_hash);
    std::map<std::string, uint32_t> scsi_nonmedium_metrics_hash = m_drive.getTapeNonMediumErrors();
    appendMetricsToScopedParams(scopedContainer, scsi_nonmedium_metrics_hash);
    logSCSIStats("Logging mount general statistics",
                 scsi_write_metrics_hash.size() + scsi_nonmedium_metrics_hash.size());
  } catch (const cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_logContext.log(cta::log::ERR, "Exception in logging mount general statistics");
  }

  // drive statistics
  try {
    cta::log::ScopedParamContainer scopedContainer(m_logContext);
    appendDriveAndTapeInfoToScopedParams(scopedContainer);
    // get drive stats
    std::map<std::string, float> scsi_quality_metrics_hash = m_drive.getQualityStats();
    appendMetricsToScopedParams(scopedContainer, scsi_quality_metrics_hash);
    std::map<std::string, uint32_t> scsi_drive_metrics_hash = m_drive.getDriveStats();
    appendMetricsToScopedParams(scopedContainer, scsi_drive_metrics_hash);
    logSCSIStats("Logging drive statistics", scsi_quality_metrics_hash.size() + scsi_drive_metrics_hash.size());
  } catch (const cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_logContext.log(cta::log::ERR, "Exception in logging drive statistics");
  }

  // volume statistics
  try {
    cta::log::ScopedParamContainer scopedContainer(m_logContext);
    appendDriveAndTapeInfoToScopedParams(scopedContainer);
    std::map<std::string, uint32_t> scsi_metrics_hash = m_drive.getVolumeStats();
    appendMetricsToScopedParams(scopedContainer, scsi_metrics_hash);
    logSCSIStats("Logging volume statistics", scsi_metrics_hash.size());
  } catch (const cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_logContext.log(cta::log::ERR, "Exception in logging volume statistics");
  }
}
