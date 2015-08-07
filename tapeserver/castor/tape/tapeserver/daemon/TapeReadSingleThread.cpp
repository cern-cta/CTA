/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
//------------------------------------------------------------------------------
//constructor for TapeReadSingleThread
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeReadSingleThread::TapeReadSingleThread(
  castor::tape::tapeserver::drive::DriveInterface& drive,
  mediachanger::MediaChangerFacade& mc,
  TapeServerReporter& initialProcess,
  const client::ClientInterface::VolumeInfo& volInfo,
  uint64_t maxFilesRequest,
  castor::server::ProcessCap& capUtils,
  RecallWatchDog& watchdog,
  castor::log::LogContext& lc) :
  TapeSingleThreadInterface<TapeReadTask>(drive, mc, initialProcess, volInfo,
    capUtils, lc),
  m_maxFilesRequest(maxFilesRequest),
  m_watchdog(watchdog) {}

//------------------------------------------------------------------------------
//TapeCleaning::~TapeCleaning()
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeReadSingleThread::TapeCleaning::~TapeCleaning() {
  // Tell everyone to wrap up the session
  // We now acknowledge to the task injector that read reached the end. There
  // will hence be no more requests for more.
  m_this.m_taskInjector->finish();
  //then we log/notify
  m_this.m_logContext.log(LOG_DEBUG, "Starting session cleanup. Signalled end of session to task injector.");
  m_this.m_stats.waitReportingTime += m_timer.secs(castor::utils::Timer::resetCounter);
  // Log (safely, exception-wise) the tape alerts (if any) at the end of the session
  try { m_this.logTapeAlerts(); } catch (...) {}
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
    const uint32_t waitMediaInDriveTimeout = 60;
    try {
      m_this.m_drive.waitUntilReady(waitMediaInDriveTimeout);
    } catch (castor::exception::Exception &) {}
    if (!m_this.m_drive.hasTapeInPlace()) {
      m_this.m_logContext.log(LOG_INFO, "TapeReadSingleThread: No tape to unload");
      goto done;
    }
    // in the special case of a "manual" mode tape, we should skip the unload too.
    if (mediachanger::TAPE_LIBRARY_TYPE_MANUAL != m_this.m_drive.config.getLibrarySlot().getLibraryType()) {
      m_this.m_drive.unloadTape();
      m_this.m_logContext.log(LOG_INFO, "TapeReadSingleThread: Tape unloaded");
    } else {
      m_this.m_logContext.log(LOG_INFO, "TapeReadSingleThread: Tape NOT unloaded (manual mode)");
    }
    m_this.m_stats.unloadTime += m_timer.secs(castor::utils::Timer::resetCounter);
    // And return the tape to the library
    // In case of manual mode, this will be filtered by the rmc daemon
    // (which will do nothing)
    currentErrorToCount = "Error_tapeDismount";
    m_this.m_mc.dismountTape(m_this.m_volInfo.vid, m_this.m_drive.config.getLibrarySlot());
    m_this.m_stats.unmountTime += m_timer.secs(castor::utils::Timer::resetCounter);
    m_this.m_logContext.log(LOG_INFO, mediachanger::TAPE_LIBRARY_TYPE_MANUAL != m_this.m_drive.config.getLibrarySlot().getLibraryType() ?
      "TapeReadSingleThread : tape unmounted":"TapeReadSingleThread : tape NOT unmounted (manual mode)");
    m_this.m_initialProcess.tapeUnmounted();
    m_this.m_stats.waitReportingTime += m_timer.secs(castor::utils::Timer::resetCounter);
  } catch(const castor::exception::Exception& ex){
    // Something failed during the cleaning 
    m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
    castor::log::ScopedParamContainer scoped(m_this.m_logContext);
    scoped.add("exception_message", ex.getMessageValue())
          .add("exception_code",ex.code());
    m_this.m_logContext.log(LOG_ERR, "Exception in TapeReadSingleThread-TapeCleaning when unmounting the tape");
    try {
      if (currentErrorToCount.size()) {
        m_this.m_watchdog.addToErrorCount(currentErrorToCount);
      }
    } catch (...) {}
  } catch (...) {
    // Notify something failed during the cleaning 
    m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
    m_this.m_logContext.log(LOG_ERR, "Non-Castor exception in TapeReadSingleThread-TapeCleaning when unmounting the tape");
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
  castor::server::BlockingQueue<TapeReadTask *>::valueRemainingPair 
    vrp = m_tasks.popGetSize();
  // If we just passed (down) the half full limit, ask for more
  // (the remaining value is after pop)
  if(vrp.remaining + 1 == m_maxFilesRequest/2) {
    // This is not a last call
    m_taskInjector->requestInjection(false);
  } else if (0 == vrp.remaining) {
    // This is a last call: if the task injector comes up empty on this
    // one, he'll call it the end.
    m_taskInjector->requestInjection(true);
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
    new castor::tape::tapeFile::ReadSession(m_drive,m_volInfo));
    m_logContext.log(LOG_DEBUG, "Created tapeFile::ReadSession with success");
    
    return rs;
  }catch(castor::exception::Exception & ex){
      castor::log::ScopedParamContainer scoped(m_logContext); 
      scoped.add("exception_message", ex.getMessageValue())
            .add("exception_code",ex.code());
      m_logContext.log(LOG_ERR, "Failed to tapeFile::ReadSession");
      throw castor::exception::Exception("Tape's label is either missing or not valid"); 
  }  
}

//-----------------------------------------------------------------------------
// volumeModeToString
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::TapeReadSingleThread::
  mountTypeToString(const cta::MountType::Enum mountType) const throw() {
  switch(mountType) {
  case cta::MountType::RETRIEVE: return "RETRIEVE";
  case cta::MountType::ARCHIVE : return "ARCHIVE";
  default                      : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
//TapeReadSingleThread::run()
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeReadSingleThread::run() {
  m_logContext.pushOrReplace(log::Param("thread", "TapeRead"));
  castor::utils::Timer timer, totalTimer;
  std::string currentErrorToCount = "Error_setCapabilities";
  try{
    // Set capabilities allowing rawio (and hence arbitrary SCSI commands)
    // through the st driver file descriptor.
    setCapabilities();
    
    // Report the parameters of the session to the main thread
    typedef castor::log::Param Param;
    m_watchdog.addParameter(Param("TPVID", m_volInfo.vid));
    m_watchdog.addParameter(Param("mountType", mountTypeToString(m_volInfo.mountType)));
    m_watchdog.addParameter(Param("density", m_volInfo.density));
    
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
      currentErrorToCount = "Error_tapeMountForRead";
      mountTapeReadOnly();
      currentErrorToCount = "Error_tapeLoad";
      waitForDrive();
      currentErrorToCount = "Error_checkingTapeAlert";
      logTapeAlerts();
      m_stats.mountTime += timer.secs(castor::utils::Timer::resetCounter);
      {
        castor::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("mountTime", m_stats.mountTime);
        m_logContext.log(LOG_INFO, "Tape mounted and drive ready");
      }
      // Then we have to initialise the tape read session
      currentErrorToCount = "Error_tapesCheckLabelBeforeReading";
      std::unique_ptr<castor::tape::tapeFile::ReadSession> rs(openReadSession());
      // From now on, the tasks will identify problems when executed.
      currentErrorToCount = "";
      m_stats.positionTime += timer.secs(castor::utils::Timer::resetCounter);
      //and then report
      {
        castor::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("positionTime", m_stats.positionTime);
        m_logContext.log(LOG_INFO, "Tape read session session successfully started");
      }
      m_initialProcess.tapeMountedForRead();
      m_stats.waitReportingTime += timer.secs(castor::utils::Timer::resetCounter);
      // Then we will loop on the tasks as they get from 
      // the task injector
      std::unique_ptr<TapeReadTask> task;
      while(true) {
        //get a task
        task.reset(popAndRequestMoreJobs());
        m_stats.waitInstructionsTime += timer.secs(castor::utils::Timer::resetCounter);
        // If we reached the end
        if (NULL==task.get()) {
          m_logContext.log(LOG_DEBUG, "No more files to read from tape");
          break;
        }
        // This can lead the the session being marked as corrupt, so we test
        // it in the while loop
        task->execute(*rs, m_logContext, m_watchdog,m_stats,timer);
        // Transmit the statistics to the watchdog thread
        m_watchdog.updateStats(m_stats);
        // The session could have been corrupted (failed positioning)
        if(rs->isCorrupted()) {
          throw castor::exception::Exception ("Session corrupted: exiting task execution loop in TapeReadSingleThread. Cleanup will follow.");
        }
      }
    }
    // The session completed successfully, and the cleaner (unmount) executed
    // at the end of the previous block. Log the results.
    log::ScopedParamContainer params(m_logContext);
    params.add("status", "success");
    m_stats.totalTime = totalTimer.secs();
    logWithStat(LOG_INFO, "Tape thread complete",
            params);
    // Report one last time the stats, after unloading/unmounting.
    m_watchdog.updateStats(m_stats);
  } catch(const castor::exception::Exception& e){
    // We can still update the session stats one last time (unmount timings
    // should have been updated by the RAII cleaner/unmounter).
    m_watchdog.updateStats(m_stats);
    // We end up here because one step failed, be it at mount time, of after
    // failing to position by fseq (this is fatal to a read session as we need
    // to know where we are to proceed to the next file incrementally in fseq
    // positioning mode).
    // This can happen late in the session, so we can still print the stats.
    log::ScopedParamContainer params(m_logContext);
    params.add("status", "error")
          .add("ErrorMesage", e.getMessageValue());
    m_stats.totalTime = totalTimer.secs();
    logWithStat(LOG_INFO, "Tape thread complete",
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
  int level, const std::string& msg, log::ScopedParamContainer& params) {
    params.add("type", "read")
          .add("TPVID", m_volInfo.vid)
          .add("mountTime", m_stats.mountTime)
          .add("positionTime", m_stats.positionTime)
          .add("waitInstructionsTime", m_stats.waitInstructionsTime)
          .add("readWriteTime", m_stats.readWriteTime)
          .add("waitFreeMemoryTime", m_stats.waitFreeMemoryTime)
          .add("waitReportingTime", m_stats.waitReportingTime)
          .add("unloadTime", m_stats.unloadTime)
          .add("unmountTime", m_stats.unmountTime)
          .add("transferTime", m_stats.transferTime())
          .add("totalTime", m_stats.totalTime)
          .add("dataVolume", m_stats.dataVolume)
          .add("headerVolume", m_stats.headerVolume)
          .add("files", m_stats.filesCount)
          .add("payloadTransferSpeedMBps", m_stats.totalTime?1.0*m_stats.dataVolume
                  /1000/1000/m_stats.totalTime:0.0)
          .add("driveTransferSpeedMBps", m_stats.totalTime?1.0*(m_stats.dataVolume+m_stats.headerVolume)
                  /1000/1000/m_stats.totalTime:0.0);
    m_logContext.log(level,msg);  
}
