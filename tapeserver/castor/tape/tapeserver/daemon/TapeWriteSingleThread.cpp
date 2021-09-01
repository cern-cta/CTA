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

#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/MigrationTaskInjector.hpp"

//------------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeWriteSingleThread::TapeWriteSingleThread(
castor::tape::tapeserver::drive::DriveInterface & drive, 
        cta::mediachanger::MediaChangerFacade & mc,
        TapeServerReporter & tsr,
        MigrationWatchDog & mwd,
        const VolumeInfo& volInfo,
        cta::log::LogContext & lc,
        MigrationReportPacker & repPacker,
        cta::server::ProcessCap &capUtils,
        uint64_t filesBeforeFlush, uint64_t bytesBeforeFlush,
        const bool useLbp, const std::string & externalEncryptionKeyScript,
        const cta::ArchiveMount & archiveMount,
        const uint64_t tapeLoadTimeout):
        TapeSingleThreadInterface<TapeWriteTask>(drive, mc, tsr, volInfo, 
          capUtils, lc, externalEncryptionKeyScript,tapeLoadTimeout),
        m_filesBeforeFlush(filesBeforeFlush),
        m_bytesBeforeFlush(bytesBeforeFlush),
        m_drive(drive),
        m_reportPacker(repPacker),
        m_lastFseq(-1),
        m_compress(true),
        m_useLbp(useLbp),
        m_watchdog(mwd),
        m_archiveMount(archiveMount){}

//------------------------------------------------------------------------------
//TapeCleaning::~TapeCleaning()
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeWriteSingleThread::TapeCleaning::~TapeCleaning(){
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
  m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::CleaningUp, cta::nullopt, m_this.m_logContext);
  // This out-of-try-catch variables allows us to record the stage of the 
  // process we're in, and to count the error if it occurs.
  // We will not record errors for an empty string. This will allow us to
  // prevent counting where error happened upstream.
  // Log (safely, exception-wise) the tape alerts (if any) at the end of the session
  try { m_this.logTapeAlerts(); } catch (...) {}
  // Log (safely, exception-wise) the tape SCSI metrics at the end of the session
  try { m_this.logSCSIMetrics(); } catch(...) {}
  m_this.m_initialProcess.reportState(cta::tape::session::SessionState::Unmounting,
      cta::tape::session::SessionType::Archive);
  std::string currentErrorToCount = "Error_tapeUnload";
  try{
    // Do the final cleanup
    // First check that a tape is actually present in the drive. We can get here
    // after failing to mount (library error) in which case there is nothing to
    // do (and trying to unmount will only lead to a failure.)
    const uint32_t waitMediaInDriveTimeout = m_this.m_tapeLoadTimeout;
    try {
      m_this.m_drive.waitUntilReady(waitMediaInDriveTimeout);
    } catch (cta::exception::TimeOut &) {}
    if (!m_this.m_drive.hasTapeInPlace()) {
      m_this.m_logContext.log(cta::log::INFO, "TapeWriteSingleThread: No tape to unload");
      goto done;
    }
    // in the special case of a "manual" mode tape, we should skip the unload too.
    if (cta::mediachanger::TAPE_LIBRARY_TYPE_MANUAL != m_this.m_drive.config.librarySlot().getLibraryType()) {
      m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unloading,cta::nullopt, m_this.m_logContext);
      m_this.m_drive.unloadTape();
      m_this.m_logContext.log(cta::log::INFO, "TapeWriteSingleThread: Tape unloaded");
    } else {
        m_this.m_logContext.log(cta::log::INFO, "TapeWriteSingleThread: Tape NOT unloaded (manual mode)");
    }
    m_this.m_stats.unloadTime += m_timer.secs(cta::utils::Timer::resetCounter);
    // And return the tape to the library
    // In case of manual mode, this will be filtered by the rmc daemon
    // (which will do nothing)
    currentErrorToCount = "Error_tapeDismount";
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, cta::nullopt, m_this.m_logContext);
    m_this.m_mc.dismountTape(m_this.m_volInfo.vid, m_this.m_drive.config.librarySlot());
    m_this.m_drive.disableLogicalBlockProtection();
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Up, cta::nullopt, m_this.m_logContext);
    m_this.m_stats.unmountTime += m_timer.secs(cta::utils::Timer::resetCounter);
    m_this.m_logContext.log(cta::log::INFO, cta::mediachanger::TAPE_LIBRARY_TYPE_MANUAL != m_this.m_drive.config.librarySlot().getLibraryType() ?
      "TapeWriteSingleThread : tape unmounted":"TapeWriteSingleThread : tape NOT unmounted (manual mode)");
    m_this.m_initialProcess.reportState(cta::tape::session::SessionState::ShuttingDown,
      cta::tape::session::SessionType::Archive);
    m_this.m_stats.waitReportingTime += m_timer.secs(cta::utils::Timer::resetCounter);
  }
  catch(const cta::exception::Exception& ex){
    // Notify something failed during the cleaning 
    m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
    const int logLevel = cta::log::ERR;
    const std::string errorMsg = "Exception in TapeWriteSingleThread-TapeCleaning. Putting the drive down.";
    cta::optional<std::string> reason = cta::common::dataStructures::DesiredDriveState::generateReasonFromLogMsg(logLevel,errorMsg);
    m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Down,reason, m_this.m_logContext);
    cta::log::ScopedParamContainer scoped(m_this.m_logContext);
    scoped.add("exceptionMessage", ex.getMessageValue());
    m_this.m_logContext.log(logLevel, errorMsg);
    // As we do not throw exceptions from here, the watchdog signalling has
    // to occur from here.
    try {
      if (currentErrorToCount.size()) {
        m_this.m_watchdog.addToErrorCount(currentErrorToCount);
      }
    } catch (...) {}
  } catch (...) {
     // Notify something failed during the cleaning 
    const int logLevel = cta::log::ERR;
    const std::string errorMsg = "Non-Castor exception in TapeWriteSingleThread-TapeCleaning when unmounting the tape. Putting the drive down.";
    cta::optional<std::string> reason = cta::common::dataStructures::DesiredDriveState::generateReasonFromLogMsg(logLevel,errorMsg);
     m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
     m_this.m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Down,reason,m_this.m_logContext);
     m_this.m_logContext.log(logLevel,errorMsg);
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
//setlastFseq
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeWriteSingleThread::
setlastFseq(uint64_t lastFseq){
  m_lastFseq=lastFseq;
}
//------------------------------------------------------------------------------
//openWriteSession
//------------------------------------------------------------------------------
std::unique_ptr<castor::tape::tapeFile::WriteSession> 
castor::tape::tapeserver::daemon::TapeWriteSingleThread::openWriteSession() {
  using cta::log::LogContext;
  using cta::log::Param;
  typedef LogContext::ScopedParam ScopedParam;
  
  std::unique_ptr<castor::tape::tapeFile::WriteSession> writeSession;
  
  ScopedParam sp[]={
    ScopedParam(m_logContext, Param("lastFseq", m_lastFseq)),
    ScopedParam(m_logContext, Param("compression", m_compress)),
    ScopedParam(m_logContext, Param("useLbp", m_useLbp)),
  };
  tape::utils::suppresUnusedVariable(sp);
  try {
    writeSession.reset(
    new castor::tape::tapeFile::WriteSession(m_drive, m_volInfo, m_lastFseq,
      m_compress, m_useLbp)
    );
  }
  catch (cta::exception::Exception & e) {
    ScopedParam sp0(m_logContext, Param("ErrorMessage", e.getMessageValue()));
    m_logContext.log(cta::log::ERR, "Failed to start tape write session");
    // TODO: log and unroll the session
    // TODO: add an unroll mode to the tape read task. (Similar to exec, but pushing blocks marked in error)
    throw;
  }
  return writeSession;
}
//------------------------------------------------------------------------------
//tapeFlush
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeWriteSingleThread::
tapeFlush(const std::string& message,uint64_t bytes,uint64_t files,
  cta::utils::Timer & timer)
{
  m_drive.flush();
  double flushTime = timer.secs(cta::utils::Timer::resetCounter);
  cta::log::ScopedParamContainer params(m_logContext);
  params.add("files", files)
        .add("bytes", bytes)
        .add("flushTime", flushTime);
  m_logContext.log(cta::log::INFO,message);
  m_stats.flushTime += flushTime;
  

  m_reportPacker.reportFlush(m_drive.getCompression(), m_logContext);
  m_drive.clearCompressionStats();
}

//------------------------------------------------------------------------
//   logAndCheckTapeAlertsForWrite
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::TapeWriteSingleThread::
logAndCheckTapeAlertsForWrite() {
  std::vector<uint16_t> tapeAlertCodes = m_drive.getTapeAlertCodes();
  if (tapeAlertCodes.empty()) return false;
  size_t alertNumber = 0;
  // Log tape alerts in the logs.
  std::vector<std::string> tapeAlerts = m_drive.getTapeAlerts(tapeAlertCodes);
  for (std::vector<std::string>::iterator ta=tapeAlerts.begin();
          ta!=tapeAlerts.end();ta++)
  {
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("tapeAlert",*ta)
          .add("tapeAlertNumber", alertNumber++)
          .add("tapeAlertCount", tapeAlerts.size());
    m_logContext.log(cta::log::WARNING, "Tape alert detected");
  }
  // Add tape alerts in the tape log parameters
  std::vector<std::string> tapeAlertsCompact = 
    m_drive.getTapeAlertsCompact(tapeAlertCodes);
  for (std::vector<std::string>::iterator tac=tapeAlertsCompact.begin();
          tac!=tapeAlertsCompact.end();tac++)
  {
    countTapeLogError(std::string("Error_")+*tac);
  }
  return(m_drive.tapeAlertsCriticalForWrite(tapeAlertCodes));
}

//------------------------------------------------------------------------------
//   isTapeWritable
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeWriteSingleThread::
isTapeWritable() const {
// check that drive is not write protected
      if(m_drive.isWriteProtected()) {   
        cta::exception::Exception ex;
        ex.getMessage() <<
                "End session with error. Drive is write protected. Aborting labelling...";
        throw ex;
      }
}

//-----------------------------------------------------------------------------
// volumeModeToString
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::TapeWriteSingleThread::
  mountTypeToString(const cta::common::dataStructures::MountType mountType) const throw() {
  switch(mountType) {
  case cta::common::dataStructures::MountType::Retrieve: return "Retrieve";
  case cta::common::dataStructures::MountType::ArchiveForUser: return "ArchiveForUser";
  case cta::common::dataStructures::MountType::ArchiveForRepack: return "ArchiveForRepack";
  case cta::common::dataStructures::MountType::Label: return "Label";
  default                      : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
//run
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeWriteSingleThread::run() {
  cta::log::ScopedParamContainer threadGlobalParams(m_logContext);
  threadGlobalParams.add("thread", "TapeWrite");
  cta::utils::Timer timer, totalTimer;
  // This out-of-try-catch variables allows us to record the stage of the 
  // process we're in, and to count the error if it occurs.
  // We will not record errors for an empty string. This will allow us to
  // prevent counting where error happened upstream.
  std::string currentErrorToCount = "Error_tapeMountForWrite";
  try
  {
    // Report the parameters of the session to the main thread
    typedef cta::log::Param Param;
    m_watchdog.addParameter(Param("tapeVid", m_volInfo.vid));
    m_watchdog.addParameter(Param("mountType", mountTypeToString(m_volInfo.mountType)));
    m_watchdog.addParameter(Param("mountId", m_volInfo.mountId));
    m_watchdog.addParameter(Param("volReqId", m_volInfo.mountId));
    
    m_watchdog.addParameter(Param("tapeDrive",m_drive.config.unitName));
    m_watchdog.addParameter(Param("vendor",m_archiveMount.getVendor()));
    m_watchdog.addParameter(Param("vo",m_archiveMount.getVo()));
    m_watchdog.addParameter(Param("mediaType",m_archiveMount.getMediaType()));
    m_watchdog.addParameter(Param("tapePool",m_archiveMount.getPoolName()));
    m_watchdog.addParameter(Param("logicalLibrary",m_drive.config.logicalLibrary));
    m_watchdog.addParameter(Param("capacityInBytes",m_archiveMount.getCapacityInBytes()));
      
    // Set the tape thread time in the watchdog for total time estimation in case
    // of crash
    m_watchdog.updateThreadTimer(totalTimer);
    
    //pair of brackets to create an artificial scope for the tape cleaning
    {
      //log and notify
      m_logContext.log(cta::log::INFO, "Starting tape write thread");
      
      // The tape will be loaded 
      // it has to be unloaded, unmounted at all cost -> RAII
      // will also take care of the TapeServerReporter
      // 
      TapeCleaning cleaner(*this, timer);
      m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Mounting,cta::nullopt, m_logContext);
      // Before anything, the tape should be mounted
      // This call does the logging of the mount
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("vo",m_archiveMount.getVo());
      params.add("mediaType",m_archiveMount.getMediaType());
      params.add("tapePool",m_archiveMount.getPoolName());
      params.add("logicalLibrary",m_drive.config.logicalLibrary);
      params.add("mountType",mountTypeToString(m_volInfo.mountType));
      params.add("vendor",m_archiveMount.getVendor());
      params.add("capacityInBytes",m_archiveMount.getCapacityInBytes());
      m_logContext.log(cta::log::INFO, "Tape session started");
      mountTapeReadWrite();
      currentErrorToCount = "Error_tapeLoad";
      cta::utils::Timer tapeLoadTimer;
      waitForDrive();
      double tapeLoadTime = tapeLoadTimer.secs();
      currentErrorToCount = "Error_checkingTapeAlert";
      if(logAndCheckTapeAlertsForWrite()) {
        throw cta::exception::Exception("Aborting migration session in"
          " presence of critical tape alerts");
      }

      currentErrorToCount = "Error_tapeNotWriteable";
      isTapeWritable();
      
      m_stats.mountTime += timer.secs(cta::utils::Timer::resetCounter);
      {
        cta::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("mountTime", m_stats.mountTime);
        scoped.add("tapeLoadTime",tapeLoadTime);
        m_logContext.log(cta::log::INFO, "Tape mounted and drive ready");
      }
      m_archiveMount.setTapeMounted(m_logContext);
      try {
        currentErrorToCount = "Error_tapeEncryptionEnable";
        // We want those scoped params to last for the whole mount.
        // This will allow each written file to be logged with its encryption
        // status:
        cta::log::ScopedParamContainer encryptionLogParams(m_logContext);
        {
          auto encryptionStatus = m_encryptionControl.enable(m_drive, m_volInfo.vid,
                                                             EncryptionControl::SetTag::SET_TAG);
 
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
      currentErrorToCount = "Error_tapePositionForWrite";
      // Then we have to initialize the tape write session
      std::unique_ptr<castor::tape::tapeFile::WriteSession> writeSession(openWriteSession());
      m_stats.positionTime  += timer.secs(cta::utils::Timer::resetCounter);
      {
        cta::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("positionTime", m_stats.positionTime);
        scoped.add("useLbp", m_useLbp);
        scoped.add("detectedLbp", writeSession->isTapeWithLbp());

        if (!writeSession->isTapeWithLbp() && m_useLbp) {
          m_logContext.log(cta::log::INFO, "Tapserver started with LBP support but "
            "the tape without LBP label mounted");
        }
        switch(m_drive.getLbpToUse()) {
          case drive::lbpToUse::crc32cReadWrite:
            m_logContext.log(cta::log::INFO, "Write session initialised with LBP"
              " crc32c in ReadWrite mode, tape VID checked and drive positioned"
              " for writing");
            break;
          case drive::lbpToUse::disabled:
            m_logContext.log(cta::log::INFO, "Write session initialised without LBP"
              ", tape VID checked and drive positioned for writing");
            break;
          default:
            m_logContext.log(cta::log::ERR, "Write session initialised with "
              "unsupported LBP method, tape VID checked and drive positioned"
              " for writing");
        }
      }

      m_initialProcess.reportState(cta::tape::session::SessionState::Running,
        cta::tape::session::SessionType::Archive);
      uint64_t bytes=0;
      uint64_t files=0;
      m_stats.waitReportingTime += timer.secs(cta::utils::Timer::resetCounter);
      // Tasks handle their error logging themselves.
      currentErrorToCount = "";
      std::unique_ptr<TapeWriteTask> task;   
      m_reportPacker.reportDriveStatus(cta::common::dataStructures::DriveStatus::Transferring,cta::nullopt, m_logContext); 
      while(1) {
        //get a task
        task.reset(m_tasks.pop());
        m_stats.waitInstructionsTime += timer.secs(cta::utils::Timer::resetCounter);
        //if is the end
        if(NULL==task.get()) {      
          //we flush without asking
          tapeFlush("No more data to write on tape, unconditional flushing to the client",bytes,files,timer);
          m_stats.flushTime += timer.secs(cta::utils::Timer::resetCounter);
          cta::log::LogContext::ScopedParam sp0(m_logContext, cta::log::Param("tapeThreadDuration", totalTimer.secs()));
          m_logContext.log(cta::log::DEBUG, "writing data to tape has finished");
          break;
        }
        task->execute(*writeSession,m_reportPacker,m_watchdog,m_logContext,timer);
        // Add the tasks counts to the session's
        m_stats.add(task->getTaskStats());
        // Transmit the statistics to the watchdog thread
        m_watchdog.updateStatsWithoutDeliveryTime(m_stats);
        // Increase local flush counters (session counters are incremented by
        // the task)
        files++;
        bytes+=task->fileSize();
        //if one flush counter is above a threshold, then we flush
        if (files >= m_filesBeforeFlush || bytes >= m_bytesBeforeFlush) {
          currentErrorToCount = "Error_tapeFlush";
          tapeFlush("Normal flush because thresholds was reached",bytes,files,timer);
          files=0;
          bytes=0;
          currentErrorToCount = "";
        }
      } //end of while(1))
    }

    // The session completed successfully, and the cleaner (unmount) executed
    // at the end of the previous block. Log the results.
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("status", "success");
    m_stats.totalTime = totalTimer.secs();
    m_stats.deliveryTime = m_stats.totalTime;
    logWithStats(cta::log::INFO, "Tape thread complete",params);
    // Report one last time the stats, after unloading/unmounting.
    m_watchdog.updateStats(m_stats);
    //end of session + log
    m_reportPacker.reportEndOfSession(m_logContext);
  } //end of try 
  catch(const cta::exception::Exception& e){
    //we end there because write session could not be opened 
    //or because a task failed or because flush failed
    
    // First off, indicate the problem to the task injector so it does not inject
    // more work in the pipeline
    // If the problem did not originate here, we just re-flag the error, and
    // this has no effect, but if we had a problem with a non-file operation
    // like mounting the tape, then we have to signal the problem to the disk
    // side and the task injector, which will trigger the end of session.
    m_injector->setErrorFlag();
    // We can still update the session stats one last time (unmount timings
    // should have been updated by the RAII cleaner/unmounter).
    m_watchdog.updateStatsWithoutDeliveryTime(m_stats);
    
    // If we reached the end of tape, this is not an error (ENOSPC)
    try {
      // If it's not the error we're looking for, we will go about our business
      // in the catch section. dynamic cast will throw, and we'll do ourselves
      // if the error code is not the one we want.
      const cta::exception::Errnum & en = 
        dynamic_cast<const cta::exception::Errnum &>(e);
      if(en.errorNumber()!= ENOSPC) {
        throw 0;
      }
      // This is indeed the end of the tape. Not an error.
      m_watchdog.setErrorCount("Info_tapeFilledUp",1);
      m_reportPacker.reportTapeFull(m_logContext);
    } catch (...) {
      // The error is not an ENOSPC, so it is, indeed, an error.
      // If we got here with a new error, currentErrorToCount will be non-empty,
      // and we will pass the error name to the watchdog.
      if(currentErrorToCount.size()) {
        m_watchdog.addToErrorCount(currentErrorToCount);
      }
    }
    
    //first empty all the tasks and circulate mem blocks
    while(1) {
      std::unique_ptr<TapeWriteTask>  task(m_tasks.pop());
      if(task.get()==NULL) {
        break;
      }
      task->circulateMemBlocks();
    }
    // Prepare the standard error codes for the session
    std::string errorMessage(e.getMessageValue());
    int errorCode(666);
    // Override if we got en ENOSPC error (end of tape)
    // This is 
    try {
      const cta::exception::Errnum & errnum = 
          dynamic_cast<const cta::exception::Errnum &> (e);
      if (ENOSPC == errnum.errorNumber()) {
        errorCode = ENOSPC;
        errorMessage = "End of migration due to tape full";
      }
    } catch (...) {}
    // then log the end of write thread
    cta::log::ScopedParamContainer params(m_logContext);
    params.add("status", "error")
          .add("ErrorMessage", errorMessage);
    m_stats.totalTime = totalTimer.secs();
    logWithStats(cta::log::INFO, "Tape thread complete",
            params);
    m_reportPacker.reportEndOfSessionWithErrors(errorMessage,errorCode, m_logContext);
  }      
}

//------------------------------------------------------------------------------
//logWithStats
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeWriteSingleThread::logWithStats(
int level,const std::string& msg, cta::log::ScopedParamContainer& params){
  params.add("type", "write")
        .add("tapeVid", m_volInfo.vid)
        .add("mountTime", m_stats.mountTime)
        .add("positionTime", m_stats.positionTime)
        .add("waitInstructionsTime", m_stats.waitInstructionsTime)
        .add("checksumingTime", m_stats.checksumingTime)
        .add("readWriteTime", m_stats.readWriteTime)
        .add("waitDataTime", m_stats.waitDataTime)
        .add("waitReportingTime", m_stats.waitReportingTime)
        .add("flushTime", m_stats.flushTime)
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
  m_logContext.log(level, msg);
}

//------------------------------------------------------------------------------
//logSCSIMetrics
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeWriteSingleThread::logSCSIMetrics() {
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
  }
  catch (const cta::exception::Exception &ex) {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add("exceptionMessage", ex.getMessageValue());
    m_logContext.log(cta::log::ERR, "Exception in logging mount general statistics");
  }

  // drive statistics
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
