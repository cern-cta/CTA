/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
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

#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
//------------------------------------------------------------------------------
//constructor for TapeReadSingleThread
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeReadSingleThread::TapeReadSingleThread(
  castor::tape::tapeserver::drive::DriveInterface& drive,
  cta::mediachanger::MediaChangerFacade& mc,
  TapeServerReporter& initialProcess,
  const VolumeInfo& volInfo,
  uint64_t maxFilesRequest,
  cta::server::ProcessCap& capUtils,
  RecallWatchDog& watchdog,
  cta::log::LogContext&  lc,
  RecallReportPacker &rrp,
  const bool useLbp,
  const bool useRAO,
  const std::string & externalEncryptionKeyScript,
  const cta::RetrieveMount& retrieveMount,
  const uint32_t tapeLoadTimeout) :
  TapeSingleThreadInterface<TapeReadTask>(drive, mc, initialProcess, volInfo,
    capUtils, lc, externalEncryptionKeyScript,tapeLoadTimeout),
  m_maxFilesRequest(maxFilesRequest),
  m_watchdog(watchdog),
  m_rrp(rrp),
  m_useLbp(useLbp),
  m_useRAO(useRAO),
  m_retrieveMount(retrieveMount){}

//------------------------------------------------------------------------------
//TapeCleaning::~TapeCleaning()
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeReadSingleThread::TapeCleaning::~TapeCleaning() {
  m_this.m_rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::CleaningUp);
  // Tell everyone to wrap up the session
  // We now acknowledge to the task injector that read reached the end. There
  // will hence be no more requests for more.
  m_this.m_taskInjector->finish();
  //then we log/notify
  m_this.m_logContext.log(cta::log::DEBUG, "Starting session cleanup. Signalled end of session to task injector.");
  m_this.m_stats.waitReportingTime += m_timer.secs(cta::utils::Timer::resetCounter);
  // Disable encryption (or at least try)
  try {
    if (m_this.m_encryptionControl.disable(m_this.m_drive))
      m_this.m_logContext.log(cta::log::INFO, "Turned encryption off before unmounting");
  } catch (cta::exception::Exception & ex) {
    cta::log::ScopedParamContainer scoped(m_this.m_logContext);
    scoped.add("exceptionError", ex.getMessageValue());
    m_this.m_logContext.log(cta::log::ERR, "Failed to turn off encryption before unmounting");
  }
  m_this.m_stats.encryptionControlTime += m_timer.secs(cta::utils::Timer::resetCounter);
  // Log (safely, exception-wise) the tape alerts (if any) at the end of the session
  try { m_this.logTapeAlerts(); } catch (...) {}
  // Log safely SCSI Metrits
  try { m_this.logSCSIMetrics(); } catch (...) {}

  // Log safely errors at the end of the session
  // This out-of-try-catch variables allows us to record the stage of the 
  // process we're in, and to count the error if it occurs.
  // We will not record errors for an empty string. This will allow us to
  // prevent counting where error happened upstream.
  std::string currentErrorToCount = "Error_tapeUnload";
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
    const uint32_t tapeLoadTimeout = m_this.m_tapeLoadTimeout;
    try {
      m_this.m_drive.waitUntilReady(tapeLoadTimeout);
    } catch (cta::exception::TimeOut &) {}
    if (!m_this.m_drive.hasTapeInPlace()) {
      m_this.m_logContext.log(cta::log::INFO, "TapeReadSingleThread: No tape to unload");
      goto done;
    }
    // in the special case of a "manual" mode tape, we should skip the unload too.
    if (cta::mediachanger::TAPE_LIBRARY_TYPE_MANUAL != m_this.m_drive.config.librarySlot().getLibraryType()) {      
      m_this.m_rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unloading);
      m_this.m_drive.unloadTape();
      m_this.m_logContext.log(cta::log::INFO, "TapeReadSingleThread: Tape unloaded");
    } else {
      m_this.m_logContext.log(cta::log::INFO, "TapeReadSingleThread: Tape NOT unloaded (manual mode)");
    }
    m_this.m_stats.unloadTime += m_timer.secs(cta::utils::Timer::resetCounter);
    // And return the tape to the library
    // In case of manual mode, this will be filtered by the rmc daemon
    // (which will do nothing)
    currentErrorToCount = "Error_tapeDismount";
    m_this.m_rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting);
    m_this.m_mc.dismountTape(m_this.m_volInfo.vid, m_this.m_drive.config.librarySlot());
    m_this.m_drive.disableLogicalBlockProtection();
    m_this.m_stats.unmountTime += m_timer.secs(cta::utils::Timer::resetCounter);
    m_this.m_logContext.log(cta::log::INFO, cta::mediachanger::TAPE_LIBRARY_TYPE_MANUAL != m_this.m_drive.config.librarySlot().getLibraryType() ?
      "TapeReadSingleThread : tape unmounted":"TapeReadSingleThread : tape NOT unmounted (manual mode)");
    m_this.m_initialProcess.reportTapeUnmountedForRetrieve();
    m_this.m_stats.waitReportingTime += m_timer.secs(cta::utils::Timer::resetCounter);
  } catch(const cta::exception::Exception& ex){
    // Something failed during the cleaning 
    m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
    const int logLevel = cta::log::ERR;
    const std::string errorMsg = "Exception in TapeReadSingleThread-TapeCleaning when unmounting the tape. Putting the drive down.";
    cta::optional<std::string> reason = cta::common::dataStructures::DesiredDriveState::generateReasonFromLogMsg(logLevel,errorMsg);
    m_this.m_rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Down,reason);
    cta::log::ScopedParamContainer scoped(m_this.m_logContext);
    scoped.add("exceptionMessage", ex.getMessageValue());
    m_this.m_logContext.log(logLevel, errorMsg);
    try {
      if (currentErrorToCount.size()) {
        m_this.m_watchdog.addToErrorCount(currentErrorToCount);
      }
    } catch (...) {}
  } catch (...) {
    // Notify something failed during the cleaning 
    m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
    const int logLevel = cta::log::ERR;
    const std::string errorMsg = "Non-Castor exception in TapeReadSingleThread-TapeCleaning when unmounting the tape. Putting the drive down.";
    cta::optional<std::string> reason = cta::common::dataStructures::DesiredDriveState::generateReasonFromLogMsg(logLevel,errorMsg);
    m_this.m_rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Down,reason);
    m_this.m_logContext.log(logLevel, errorMsg);
    try {
      if (currentErrorToCount.size()) {
        m_this.m_watchdog.addToErrorCount(currentErrorToCount);
      }
    } catch (...) {}
  }
  done:
  //then we terminate the global status reporter
  m_this.m_initialProcess.finish();
}

//------------------------------------------------------------------------------
//TapeReadSingleThread::popAndRequestMoreJobs()
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeReadTask *
castor::tape::tapeserver::daemon::TapeReadSingleThread::popAndRequestMoreJobs(){
  cta::threading::BlockingQueue<TapeReadTask *>::valueRemainingPair 
    vrp = m_tasks.popGetSize();
  // If we just passed (down) the half full limit, ask for more
  // (the remaining value is after pop)
  if(0 == vrp.remaining) {
    // This is a last call: if the task injector comes up empty on this
    // one, he'll call it the end.
    m_taskInjector->requestInjection(true);
  } else if (vrp.remaining + 1 == m_maxFilesRequest/2) {
    // This is not a last call
    m_taskInjector->requestInjection(false);
  }
  return vrp.value;
}

//------------------------------------------------------------------------------
//TapeReadSingleThread::openReadSession()
//------------------------------------------------------------------------------
std::unique_ptr<castor::tape::tapeFile::ReadSession>
castor::tape::tapeserver::daemon::TapeReadSingleThread::openReadSession() {
  try{
    std::unique_ptr<castor::tape::tapeFile::ReadSession> rs(
    new castor::tape::tapeFile::ReadSession(m_drive,m_volInfo, m_useLbp));
    //m_logContext.log(cta::log::DEBUG, "Created tapeFile::ReadSession with success");
    
    return rs;
  }catch(cta::exception::Exception & ex){
      cta::log::ScopedParamContainer scoped(m_logContext); 
      scoped.add("exceptionMessage", ex.getMessageValue());
      m_logContext.log(cta::log::ERR, "Failed to tapeFile::ReadSession");
      throw cta::exception::Exception("Tape's label is either missing or not valid"); 
  }  
}

//-----------------------------------------------------------------------------
// volumeModeToString
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::TapeReadSingleThread::
  mountTypeToString(const cta::common::dataStructures::MountType mountType) const throw() {
  switch(mountType) {
  case cta::common::dataStructures::MountType::Retrieve: return "Retrieve";
  case cta::common::dataStructures::MountType::ArchiveForUser : return "ArchiveForUser";
  case cta::common::dataStructures::MountType::ArchiveForRepack : return "ArchiveForRepack";
  case cta::common::dataStructures::MountType::Label: return "Label";
  default                      : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
//TapeReadSingleThread::run()
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeReadSingleThread::run() {
  m_logContext.pushOrReplace(cta::log::Param("thread", "TapeRead"));
  cta::utils::Timer timer, totalTimer;
  std::string currentErrorToCount = "Error_tapeMountForRead";
  try{
    // Report the parameters of the session to the main thread
    typedef cta::log::Param Param;
    m_watchdog.addParameter(Param("tapeVid", m_volInfo.vid));
    m_watchdog.addParameter(Param("mountType", mountTypeToString(m_volInfo.mountType)));
    m_watchdog.addParameter(Param("mountId", m_volInfo.mountId));
    m_watchdog.addParameter(Param("tapeDrive",m_drive.config.unitName));
    m_watchdog.addParameter(Param("vendor",m_retrieveMount.getVendor()));
    m_watchdog.addParameter(Param("volReqId", m_volInfo.mountId));
    m_watchdog.addParameter(Param("vo",m_retrieveMount.getVo()));
    m_watchdog.addParameter(Param("mediaType",m_retrieveMount.getMediaType()));
    m_watchdog.addParameter(Param("tapePool",m_retrieveMount.getPoolName()));
    m_watchdog.addParameter(Param("logicalLibrary",m_drive.config.logicalLibrary));
    m_watchdog.addParameter(Param("capacityInBytes",m_retrieveMount.getCapacityInBytes()));
    
    // Set the tape thread time in the watchdog for total time estimation in case
    // of crash
    m_watchdog.updateThreadTimer(totalTimer);
    
    //pair of brackets to create an artificial scope for the tapeCleaner
    {  
      // The tape will be loaded 
      // it has to be unloaded, unmounted at all cost -> RAII
      // will also take care of the TapeServerReporter and of RecallTaskInjector
      TapeCleaning tapeCleaner(*this, timer);
      // Before anything, the tape should be mounted
      m_rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Mounting);
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("vo",m_retrieveMount.getVo());
      params.add("mediaType",m_retrieveMount.getMediaType());
      params.add("tapePool",m_retrieveMount.getPoolName());
      params.add("logicalLibrary",m_drive.config.logicalLibrary);
      params.add("mountType",mountTypeToString(m_volInfo.mountType));
      params.add("vendor",m_retrieveMount.getVendor());
      params.add("capacityInBytes",m_retrieveMount.getCapacityInBytes());
      m_logContext.log(cta::log::INFO, "Tape session started");
      mountTapeReadOnly();
      currentErrorToCount = "Error_tapeLoad";
      cta::utils::Timer tapeLoadTimer;
      waitForDrive();
      double tapeLoadTime = tapeLoadTimer.secs();
      currentErrorToCount = "Error_checkingTapeAlert";
      logTapeAlerts();
      m_stats.mountTime += timer.secs(cta::utils::Timer::resetCounter);
      {
        cta::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("mountTime", m_stats.mountTime);
        scoped.add("tapeLoadTime",tapeLoadTime);
        m_logContext.log(cta::log::INFO, "Tape mounted and drive ready");
      }
      m_retrieveMount.setTapeMounted(m_logContext);
      try {
        currentErrorToCount = "Error_tapeEncryptionEnable";
        // We want those scoped params to last for the whole mount.
        // This will allow each session to be logged with its encryption
        // status:
        cta::log::ScopedParamContainer encryptionLogParams(m_logContext);
        {
          auto encryptionStatus = m_encryptionControl.enable(m_drive, m_volInfo.vid,
                                                             EncryptionControl::SetTag::NO_SET_TAG);
          if (encryptionStatus.on) {
            encryptionLogParams.add("encryption", "on")
              .add("encryptionKey", encryptionStatus.keyName)
              .add("stdout", encryptionStatus.stdout);
            m_logContext.log(cta::log::INFO, "Drive encryption enabled for this mount");
          } else {
            encryptionLogParams.add("encryption", "off");
            m_logContext.log(cta::log::INFO, "Drive encryption not enabled for this mount");
          }
        }
        m_stats.encryptionControlTime += timer.secs(cta::utils::Timer::resetCounter);
      }
      catch (cta::exception::Exception &ex) {
        cta::log::ScopedParamContainer params(m_logContext);
        params.add("ErrorMessage", ex.getMessage().str());
        m_logContext.log(cta::log::ERR, "Drive encryption could not be enabled for this mount.");
        throw;
      }
      if (m_useRAO) {
        /* Give the RecallTaskInjector access to the drive to perform RAO query */
        m_taskInjector->setPromise();
      }
      // Then we have to initialise the tape read session
      currentErrorToCount = "Error_tapesCheckLabelBeforeReading";
      std::unique_ptr<castor::tape::tapeFile::ReadSession> rs(openReadSession());
      // From now on, the tasks will identify problems when executed.
      currentErrorToCount = "";
      m_stats.positionTime += timer.secs(cta::utils::Timer::resetCounter);
      //and then report
      {
        cta::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("positionTime", m_stats.positionTime);
        scoped.add("useLbp", m_useLbp);
        scoped.add("detectedLbp", rs->isTapeWithLbp());
        if (rs->isTapeWithLbp() && !m_useLbp) {
          m_logContext.log(cta::log::WARNING, "Tapserver started without LBP support"
          " but the tape with LBP label mounted");
        }
        switch(m_drive.getLbpToUse()) {
          case drive::lbpToUse::crc32cReadOnly:
            m_logContext.log(cta::log::INFO, "Tape read session session with LBP "
              "crc32c in ReadOnly mode successfully started");
            break;
          case drive::lbpToUse::disabled:
            m_logContext.log(cta::log::INFO, "Tape read session session without LBP "
            "successfully started");
            break;
          default:
            m_logContext.log(cta::log::ERR, "Tape read session session with "
            "unsupported LBP started");
        }
      }
      m_initialProcess.reportState(cta::tape::session::SessionState::Running,
        cta::tape::session::SessionType::Retrieve);
      m_stats.waitReportingTime += timer.secs(cta::utils::Timer::resetCounter);
      // Then we will loop on the tasks as they get from 
      // the task injector
      
      // We wait the task injector to finish inserting its first batch
      // before launching the loop.
      // We do it with a promise
      m_taskInjector->waitForFirstTasksInjectedPromise();
      std::unique_ptr<TapeReadTask> task;
      m_rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Transferring);
      while(true) {
        //get a task
        task.reset(popAndRequestMoreJobs());
        m_stats.waitInstructionsTime += timer.secs(cta::utils::Timer::resetCounter);
        // If we reached the end
        if (NULL==task.get()) {
          m_logContext.log(cta::log::DEBUG, "No more files to read from tape");
          break;
        }
        // This can lead the the session being marked as corrupt, so we test
        // it in the while loop
        task->execute(*rs, m_logContext, m_watchdog,m_stats,timer);
        // Transmit the statistics to the watchdog thread
        m_watchdog.updateStatsWithoutDeliveryTime(m_stats);
        // The session could have been corrupted (failed positioning)
        if(rs->isCorrupted()) {
          throw cta::exception::Exception ("Session corrupted: exiting task execution loop in TapeReadSingleThread. Cleanup will follow.");
        }
      }
    }

    // The session completed successfully, and the cleaner (unmount) executed
    // at the end of the previous block. Log the results.
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("status", "success");
    m_stats.totalTime = totalTimer.secs();
    m_rrp.setTapeDone();
    m_rrp.setTapeComplete();
    logWithStat(cta::log::INFO, "Tape thread complete",
            params);
    // Report one last time the stats, after unloading/unmounting.
    m_watchdog.updateStatsWithoutDeliveryTime(m_stats);
  } catch(const cta::exception::Exception& e){
    // We can still update the session stats one last time (unmount timings
    // should have been updated by the RAII cleaner/unmounter).
    m_watchdog.updateStats(m_stats);
    // We end up here because one step failed, be it at mount time, of after
    // failing to position by fseq (this is fatal to a read session as we need
    // to know where we are to proceed to the next file incrementally in fseq
    // positioning mode).
    // This can happen late in the session, so we can still print the stats.
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("status", "error")
          .add("ErrorMessage", e.getMessageValue());
    m_stats.totalTime = totalTimer.secs();
    logWithStat(cta::log::INFO, "Tape thread complete",
            params);
    // Also transmit the error step to the watchdog
    if (currentErrorToCount.size()) {
      m_watchdog.addToErrorCount(currentErrorToCount);
    }
    // Flush the remaining tasks to cleanly exit.
    while(1){
      TapeReadTask* task=m_tasks.pop();
      if(!task) {
        break;
      }
      task->reportCancellationToDiskTask();
      delete task;
    }
  }  
}

//------------------------------------------------------------------------------
//TapeReadSingleThread::logWithStat()
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeReadSingleThread::logWithStat(
  int level, const std::string& msg, cta::log::ScopedParamContainer& params) {
    params.add("type", "read")
          .add("tapeVid", m_volInfo.vid)
          .add("mountTime", m_stats.mountTime)
          .add("positionTime", m_stats.positionTime)
          .add("waitInstructionsTime", m_stats.waitInstructionsTime)
          .add("readWriteTime", m_stats.readWriteTime)
          .add("waitFreeMemoryTime", m_stats.waitFreeMemoryTime)
          .add("waitReportingTime", m_stats.waitReportingTime)
          .add("unloadTime", m_stats.unloadTime)
          .add("unmountTime", m_stats.unmountTime)
          .add("encryptionControlTime", m_stats.encryptionControlTime)
          .add("transferTime", m_stats.transferTime())
          .add("totalTime", m_stats.totalTime)
          .add("dataVolume", m_stats.dataVolume)
          .add("headerVolume", m_stats.headerVolume)
          .add("files", m_stats.filesCount)
          .add("payloadTransferSpeedMBps", m_stats.totalTime?1.0*m_stats.dataVolume
                  /1000/1000/m_stats.totalTime:0.0)
          .add("driveTransferSpeedMBps", m_stats.totalTime?1.0*(m_stats.dataVolume+m_stats.headerVolume)
                  /1000/1000/m_stats.totalTime:0.0);
    m_logContext.moveToTheEndIfPresent("status");
    m_logContext.log(level,msg);  
}

//------------------------------------------------------------------------------
//logSCSIMetrics
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeReadSingleThread::logSCSIMetrics() {
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
  }
  catch (const cta::exception::Exception &ex) {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add("exceptionMessage", ex.getMessageValue());
    m_logContext.log(cta::log::ERR, "Exception in logging mount general statistics");
  }

  // drive statistic
  try {
    cta::log::ScopedParamContainer scopedContainer(m_logContext);
    appendDriveAndTapeInfoToScopedParams(scopedContainer);
    // get drive stats
    std::map<std::string,float> scsi_quality_metrics_hash = m_drive.getQualityStats();
    appendMetricsToScopedParams(scopedContainer, scsi_quality_metrics_hash);
    std::map<std::string,uint32_t> scsi_drive_metrics_hash = m_drive.getDriveStats();
    appendMetricsToScopedParams(scopedContainer, scsi_drive_metrics_hash);
    logSCSIStats("Logging drive statistics",
      scsi_quality_metrics_hash.size()+scsi_drive_metrics_hash.size());
  }
  catch (const cta::exception::Exception &ex) {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add("exceptionMessage", ex.getMessageValue());
    m_logContext.log(cta::log::ERR, "Exception in logging drive statistics");
  }

  // volume statistics
  try {
    cta::log::ScopedParamContainer scopedContainer(m_logContext);
    appendDriveAndTapeInfoToScopedParams(scopedContainer);
    std::map<std::string,uint32_t> scsi_metrics_hash = m_drive.getVolumeStats();
    appendMetricsToScopedParams(scopedContainer, scsi_metrics_hash);
    logSCSIStats("Logging volume statistics", scsi_metrics_hash.size());
  }
  catch (const cta::exception::Exception &ex) {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add("exceptionMessage", ex.getMessageValue());
    m_logContext.log(cta::log::ERR, "Exception in logging volume statistics");
  }
}

