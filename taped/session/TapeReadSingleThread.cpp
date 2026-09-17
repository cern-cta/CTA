/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeReadSingleThread.hpp"

#include "RecallTaskInjector.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "taped/drive/DriveInterface.hpp"
#include "taped/file/ReadSession.hpp"
#include "taped/file/ReadSessionFactory.hpp"

#include <optional>

//------------------------------------------------------------------------------
// Constructor for TapeReadSingleThread
//------------------------------------------------------------------------------
cta::tape::daemon::TapeReadSingleThread::TapeReadSingleThread(cta::tape::drive::DriveInterface& drive,
                                                              cta::mediachanger::MediaChangerFacade& mediaChanger,
                                                              TapeSessionTracker& tracker,
                                                              const VolumeInfo& volInfo,
                                                              uint64_t maxFilesRequest,
                                                              const cta::log::LogContext& logContext,
                                                              RecallReportPacker& reportPacker,
                                                              const bool useLbp,
                                                              const bool useRAO,
                                                              const bool useEncryption,
                                                              const std::string& externalEncryptionKeyScript,
                                                              const cta::RetrieveMount& retrieveMount,
                                                              const uint32_t tapeLoadTimeout,
                                                              cta::catalogue::Catalogue& catalogue)
    : TapeSingleThreadInterface<TapeReadTask>(drive,
                                              mediaChanger,
                                              tracker,
                                              volInfo,
                                              logContext,
                                              useEncryption,
                                              externalEncryptionKeyScript,
                                              tapeLoadTimeout),
      m_maxFilesRequest(maxFilesRequest),
      m_reportPacker(reportPacker),
      m_useLbp(useLbp),
      m_useRAO(useRAO),
      m_retrieveMount(retrieveMount),
      m_catalogue(catalogue) {}

//------------------------------------------------------------------------------
//TapeCleaning::~TapeCleaning()
//------------------------------------------------------------------------------
cta::tape::daemon::TapeReadSingleThread::TapeCleaning::~TapeCleaning() {
  m_this.m_tracker.reportState(cta::tape::session::TapeSessionState::Finalizing);
  m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::CleaningUp,
                                          std::nullopt,
                                          m_this.m_logContext);

  // Tell everyone to wrap up the session
  // We now acknowledge to the task injector that read reached the end. There
  // will hence be no more requests for more.
  m_this.m_taskInjector->finish();
  //then we log/notify
  m_this.m_logContext.log(cta::log::DEBUG, "Starting read session cleanup. Signalled end of session to task injector.");
  m_this.m_tracker.addDiskTransferStats({.waitReportingTime = m_timer.secs(cta::utils::Timer::resetCounter)});

  // Disable encryption (or at least try)
  try {
    if (m_this.m_encryptionControl.disable(m_this.m_drive)) {
      m_this.m_logContext.log(cta::log::INFO, "Turned encryption off before unmounting");
    }
  } catch (cta::exception::Exception& ex) {
    m_this.m_tracker.incrementError(TapeSessionError::TapeEncryptionDisable);
    m_this.m_tracker.setOutcome(TapeSessionOutcome::Failure);
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
      m_this.m_logContext.log(cta::log::INFO, "TapeReadSingleThread: No tape to unload");
      m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Up,
                                              std::nullopt,
                                              m_this.m_logContext);

      return;
    }

    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unloading,
                                            std::nullopt,
                                            m_this.m_logContext);
    m_this.m_tracker.reportState(cta::tape::session::TapeSessionState::Unloading);
    m_this.m_drive.unloadTape();
    m_this.m_tracker.reportState(cta::tape::session::TapeSessionState::Finalizing);
    m_this.m_logContext.log(cta::log::INFO, "TapeReadSingleThread: Tape unloaded");
    m_this.m_tracker.addTapeCleanupStats({.unloadTime = m_timer.secs(cta::utils::Timer::resetCounter)});

    // And return the tape to the library
    currentErrorToCount = TapeSessionError::TapeDismount;
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting,
                                            std::nullopt,
                                            m_this.m_logContext);

    const auto librarySlot = cta::mediachanger::LibrarySlotParser::parse(m_this.m_drive.info.rawLibrarySlot);
    m_this.m_tracker.reportState(cta::tape::session::TapeSessionState::Unmounting);
    m_this.m_mediaChanger.dismountTape(m_this.m_volInfo.vid, librarySlot);
    m_this.m_tracker.reportState(cta::tape::session::TapeSessionState::Finalizing);
    m_this.m_drive.disableLogicalBlockProtection();
    m_this.m_logContext.log(cta::log::INFO, "TapeReadSingleThread : tape unmounted");
    m_this.m_tracker.addTapeCleanupStats({.unmountTime = m_timer.secs(cta::utils::Timer::resetCounter)});

    // Report drive UP if disk threads are done
    // Report drive DrainingToDisk if there are disk write threads still active
    if (m_this.m_reportPacker.allThreadsDone()) {
      m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Up,
                                              std::nullopt,
                                              m_this.m_logContext);
    } else {
      m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::DrainingToDisk,
                                              std::nullopt,
                                              m_this.m_logContext);
    }

    m_this.m_tracker.addDiskTransferStats({.waitReportingTime = m_timer.secs(cta::utils::Timer::resetCounter)});
  } catch (const cta::exception::Exception& ex) {
    // Notify something failed during the cleaning
    m_this.m_hardwareStatus = DriveUsability::MustRemainDown;
    const int logLevel = cta::common::dataStructures::driveDownReasonSeverity(
      cta::common::dataStructures::DriveDownReason::TapeCleanupFailed);
    const std::string errorMsg =
      "Exception in TapeReadSingleThread-TapeCleaning when unmounting/unloading the tape. Putting the drive down.";
    std::optional<std::string> reason = cta::common::dataStructures::formatDriveDownReason(
      cta::common::dataStructures::DriveDownReason::TapeCleanupFailed);
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Down,
                                            reason,
                                            m_this.m_logContext);

    m_this.m_tracker.setOutcome(TapeSessionOutcome::Failure);
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
    m_this.m_hardwareStatus = DriveUsability::MustRemainDown;
    const int logLevel = cta::common::dataStructures::driveDownReasonSeverity(
      cta::common::dataStructures::DriveDownReason::TapeCleanupFailed);
    const std::string errorMsg =
      "Non-CTA exception in TapeReadSingleThread-TapeCleaning when unmounting the tape. Putting the drive down.";
    std::optional<std::string> reason = cta::common::dataStructures::formatDriveDownReason(
      cta::common::dataStructures::DriveDownReason::TapeCleanupFailed);
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Down,
                                            reason,
                                            m_this.m_logContext);

    m_this.m_tracker.setOutcome(TapeSessionOutcome::Failure);
    m_this.m_logContext.log(logLevel, errorMsg);
    try {
      m_this.m_tracker.incrementError(currentErrorToCount);
    } catch (...) {}
  }
  m_this.m_tracker.reportState(cta::tape::session::TapeSessionState::Finalizing);
}

//------------------------------------------------------------------------------
//TapeReadSingleThread::popAndRequestMoreJobs()
//------------------------------------------------------------------------------
cta::tape::daemon::TapeReadTask* cta::tape::daemon::TapeReadSingleThread::popAndRequestMoreJobs() {
  // Take the next task for the tape thread to execute and check how many left.
  // m_tasks queue gets more tasks when requestInjection() is called.
  // The queue may contain many small files that will be processed quickly
  // or a few big files that take time. We define several thresholds to make injection in time

  cta::threading::BlockingQueue<TapeReadTask*>::valueRemainingPair vrp = m_tasks.popGetSize();
  if (vrp.remaining == 0) {
    // This is a last call: the task injector will make the last attempt to fetch more jobs.
    // In any case, the injector thread will terminate
    m_taskInjector->requestInjection(true);
  } else if (vrp.remaining == m_maxFilesRequest / 2 - 1) {
    // This is not a last call: we just passed half of the maximum file limit.
    // Probably there are many small files queued, that will be processed quickly,
    // so we need to request injection of new batch of tasks early
    m_taskInjector->requestInjection(false);
  } else if (vrp.remaining == 10) {
    // This is not a last call: we are close to the end of current tasks queue.
    // 10 is a magic number that will allow us to get new tasks
    // before we are done with current batch
    m_taskInjector->requestInjection(false);
  } else if (vrp.remaining == 1) {
    // This is not a last call: given there is only one big file in the queue,
    // it's time to request the next batch
    m_taskInjector->requestInjection(false);
  }
  return vrp.value;
}

//------------------------------------------------------------------------------
// TapeReadSingleThread::openReadSession()
//------------------------------------------------------------------------------
std::unique_ptr<cta::tape::tapeFile::ReadSession> cta::tape::daemon::TapeReadSingleThread::openReadSession() {
  try {
    auto readSession = cta::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, m_useLbp);
    return readSession;
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_logContext.log(cta::log::ERR, "Failed to tapeFile::ReadSession");
    throw cta::exception::Exception("Tape's label is either missing or not valid");
  }
}

//------------------------------------------------------------------------------
//TapeReadSingleThread::run()
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeReadSingleThread::run() {
  cta::log::ScopedParamContainer threadGlobalParams(m_logContext);
  threadGlobalParams.add("thread", "TapeRead");
  cta::utils::Timer timer, totalTimer;
  // This out-of-try-catch variables allows us to record the stage of the
  // process we're in, and to count the error if it occurs.
  // Stop counting the current stage once individual tasks take over error reporting.
  TapeSessionError currentErrorToCount = TapeSessionError::TapeMountForRead;
  bool countCurrentError = true;
  try {
    // Pair of brackets to create an artificial scope for the tapeCleaner
    {
      // Log and notify
      m_logContext.log(cta::log::INFO, "Starting tape read thread");

      // The tape will be loaded
      // it has to be unloaded, unmounted at all cost -> RAII
      // will also take care of the TapeSessionReporter and of RecallTaskInjector
      TapeCleaning tapeCleaner(*this, timer);

      // Before anything, the tape should be mounted
      m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Mounting, std::nullopt, m_logContext);

      std::ostringstream ossLabelFormat;
      ossLabelFormat << std::showbase << std::internal << std::setfill('0') << std::hex << std::setw(4)
                     << static_cast<unsigned int>(m_volInfo.labelFormat);

      cta::log::ScopedParamContainer params(m_logContext);
      params.add("mediaType", m_retrieveMount.getMediaType());
      params.add("logicalLibrary", m_drive.info.logicalLibrary);
      params.add("mountType", toCamelCaseString(m_volInfo.mountType));
      params.add("labelFormat", ossLabelFormat.str());
      params.add("vendor", m_retrieveMount.getVendor());
      params.add("capacityInBytes", m_retrieveMount.getCapacityInBytes());
      m_logContext.log(cta::log::INFO, "Tape session started for read");

      currentErrorToCount = TapeSessionError::TapeLoad;
      m_tracker.reportState(cta::tape::session::TapeSessionState::Mounting);
      measureSetupTime(&TapeSetupStats::initialMountTime, [&] { mountTapeReadOnly(); });
      m_tracker.reportState(cta::tape::session::TapeSessionState::Loading);
      measureSetupTime(&TapeSetupStats::tapeLoadTime, [&] { waitForDrive(); });
      m_tracker.reportState(cta::tape::session::TapeSessionState::Preparing);
      const double tapeLoadTime = m_tracker.stats().setup.tapeLoadTime;
      currentErrorToCount = TapeSessionError::CheckingTapeAlert;
      logTapeAlerts();
      m_tracker.addTapeSetupStats({.mountTime = timer.secs(cta::utils::Timer::resetCounter)});
      {
        cta::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("mountTime", m_tracker.stats().setup.mountTime);
        scoped.add("tapeLoadTime", tapeLoadTime);
        m_logContext.log(cta::log::INFO, "Tape mounted and drive ready");
      }
      m_retrieveMount.setTapeMounted(m_logContext);
      try {
        currentErrorToCount = TapeSessionError::TapeEncryptionEnable;
        // We want those scoped params to last for the whole mount.
        // This will allow each session to be logged with its encryption
        // status:
        cta::log::ScopedParamContainer encryptionLogParams(m_logContext);
        {
          auto encryptionStatus = m_encryptionControl.enable(m_drive, m_volInfo, m_catalogue, false);
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
      if (m_useRAO) {
        /* Give the RecallTaskInjector access to the drive to perform RAO query */
        m_taskInjector->setPromise();
      }
      // Then we have to initialise the tape read session
      currentErrorToCount = TapeSessionError::TapesCheckLabelBeforeReading;
      auto readSession = openReadSession();
      m_tracker.addTapeSetupStats({.positionTime = timer.secs(cta::utils::Timer::resetCounter)});
      // and then report
      {
        cta::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("positionTime", m_tracker.stats().setup.positionTime);
        scoped.add("useLbp", m_useLbp);
        scoped.add("detectedLbp", readSession->isTapeWithLbp());

        if (readSession->isTapeWithLbp() && !m_useLbp) {
          m_logContext.log(cta::log::WARNING,
                           "Taped started without LBP support"
                           " but the tape with LBP label mounted");
        }
        switch (m_drive.getLbpToUse()) {
          case drive::lbpToUse::crc32cReadOnly:
            m_logContext.log(cta::log::INFO,
                             "Tape read session session with LBP "
                             "crc32c in ReadOnly mode successfully started");
            break;
          case drive::lbpToUse::disabled:
            m_logContext.log(cta::log::INFO,
                             "Tape read session session without LBP "
                             "successfully started");
            break;
          default:
            m_logContext.log(cta::log::ERR,
                             "Tape read session session with "
                             "unsupported LBP started");
        }
      }

      m_tracker.addDiskTransferStats({.waitReportingTime = timer.secs(cta::utils::Timer::resetCounter)});
      // Then we will loop on the tasks as they get from
      // the task injector

      // We wait the task injector to finish inserting its first batch
      // before launching the loop.
      // We do it with a promise
      m_taskInjector->waitForFirstTasksInjectedPromise();
      // From now on, the tasks will identify problems when executed.
      countCurrentError = false;
      std::unique_ptr<TapeReadTask> task;
      m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Transferring,
                                       std::nullopt,
                                       m_logContext);

      m_tracker.reportState(cta::tape::session::TapeSessionState::Transferring);
      while (true) {
        // get a task
        task.reset(popAndRequestMoreJobs());
        m_stats.waitInstructionsTime += timer.secs(cta::utils::Timer::resetCounter);
        // If we reached the end
        if (nullptr == task) {
          m_logContext.log(cta::log::DEBUG, "No more files to read from tape");
          break;
        }
        // This can lead the session being marked as corrupt, so we test it in the while loop
        task->execute(*readSession, m_logContext, m_tracker, m_stats, timer);
        m_tracker.updateTapeTransferStats(m_stats);
        // The session could have been corrupted (failed positioning)
        if (readSession->isCorrupted()) {
          throw cta::exception::Exception(
            "Session corrupted: exiting task execution loop in TapeReadSingleThread. Cleanup will follow.");
        }
      }
    }

    // The session completed successfully, and the cleaner (unmount) executed
    // at the end of the previous block. Log the results.
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("status", m_tracker.errorHappened() ? "error" : "success");
    m_totalTime = totalTimer.secs();
    m_tracker.setTotalTime(m_totalTime);
    logWithStat(cta::log::INFO, "Tape thread complete", params);
    // Report one last time the stats, after unloading/unmounting.
    m_tracker.updateTapeTransferStats(m_stats);

    // End of session and log are reported by the last active disk thread
    // in DiskWriteThreadPool::DiskWriteWorkerThread::run() if it finishes after this TapeReadSingleThread
    // Otherwise end of session is reported here
    m_reportPacker.setTapeDone();
    m_reportPacker.setTapeComplete();

    if (m_reportPacker.allThreadsDone()) {
      // If disk threads finished before (for example, due to write error), report end of session
      if (!m_tracker.errorHappened()) {
        m_reportPacker.reportEndOfSession(m_logContext);
        m_logContext.log(
          cta::log::INFO,
          "Both DiskWriteWorkerThread and TapeReadSingleThread existed, reported a successful end of session");
      } else {
        m_reportPacker.reportEndOfSessionWithErrors("End of recall session with error(s)", m_logContext);
      }
    }
  } catch (const std::exception& e) {
    const auto* ctaException = dynamic_cast<const cta::exception::Exception*>(&e);
    // Operational runtime errors need the same task draining as CTA failures.
    if (!ctaException && !dynamic_cast<const std::runtime_error*>(&e)) {
      throw;
    }
    m_tracker.setOutcome(TapeSessionOutcome::Failure);
    // Publish transfer statistics; the RAII cleaner recorded cleanup timings independently.
    m_tracker.updateTapeTransferStats(m_stats);
    // We end up here because one step failed, be it at mount time, of after
    // failing to position by fseq (this is fatal to a read session as we need
    // to know where we are to proceed to the next file incrementally in fseq
    // positioning mode).
    // This can happen late in the session, so we can still print the stats.
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("status", "error")
      .add(cta::semconv::log::exceptionMessage, (ctaException ? ctaException->getMessageValue() : e.what()));
    m_totalTime = totalTimer.secs();
    m_tracker.setTotalTime(m_totalTime);
    logWithStat(cta::log::ERR, "Tape thread complete for reading", params);
    // Also transmit the error step to the session tracker.
    if (countCurrentError) {
      m_tracker.incrementError(currentErrorToCount);
    }
    // Flush the remaining tasks to cleanly exit.
    while (true) {
      TapeReadTask* task = m_tasks.pop();
      if (!task) {
        break;
      }
      task->reportCancellationToDiskTask();
      delete task;
    }

    // Notify tape thread is finished to gracefully end the session
    m_reportPacker.setTapeDone();
    m_reportPacker.setTapeComplete();

    if (m_reportPacker.allThreadsDone()) {
      // If disk threads finished before (for example, due to write error), report end of session
      if (!m_tracker.errorHappened()) {
        m_reportPacker.reportEndOfSession(m_logContext);
        m_logContext.log(
          cta::log::INFO,
          "Both DiskWriteWorkerThread and TapeReadSingleThread existed, reported a successful end of session");
      } else {
        m_reportPacker.reportEndOfSessionWithErrors("End of recall session with error(s)", m_logContext);
      }
    }
  }
  // Both the normal path and handled failures have finished tape cleanup and task cancellation.
  m_tracker.notifyTapeDone();
}

//------------------------------------------------------------------------------
//TapeReadSingleThread::logWithStat()
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeReadSingleThread::logWithStat(int level,
                                                          const std::string& msg,
                                                          cta::log::ScopedParamContainer& params) {
  const auto sessionStats = m_tracker.stats();
  params.add("type", "read")
    .add("tapeVid", m_volInfo.vid)
    .add("mountTime", sessionStats.setup.mountTime)
    .add("initialMountTime", sessionStats.setup.initialMountTime)
    .add("tapeLoadTime", sessionStats.setup.tapeLoadTime)
    .add("positionTime", sessionStats.setup.positionTime + m_stats.positionTime)
    .add("waitInstructionsTime", m_stats.waitInstructionsTime)
    .add("readWriteTime", m_stats.readWriteTime)
    .add("waitFreeMemoryTime", m_stats.waitFreeMemoryTime)
    .add("waitReportingTime", sessionStats.disk.waitReportingTime)
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
//logSCSIMetrics
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeReadSingleThread::logSCSIMetrics() {
  try {
    // mount general statistics
    cta::log::ScopedParamContainer scopedContainer(m_logContext);
    appendDriveAndTapeInfoToScopedParams(scopedContainer);
    // get mount general stats
    std::map<std::string, uint64_t> scsi_read_metrics_hash = m_drive.getTapeReadErrors();
    appendMetricsToScopedParams(scopedContainer, scsi_read_metrics_hash);
    std::map<std::string, uint32_t> scsi_nonmedium_metrics_hash = m_drive.getTapeNonMediumErrors();
    appendMetricsToScopedParams(scopedContainer, scsi_nonmedium_metrics_hash);
    logSCSIStats("Logging mount general statistics",
                 scsi_read_metrics_hash.size() + scsi_nonmedium_metrics_hash.size());
  } catch (const cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_logContext.log(cta::log::ERR, "Exception in logging mount general statistics");
  }

  // drive statistic
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
