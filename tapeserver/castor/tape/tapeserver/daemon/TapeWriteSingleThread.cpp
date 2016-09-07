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

#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/MigrationTaskInjector.hpp"

//------------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeWriteSingleThread::TapeWriteSingleThread(
castor::tape::tapeserver::drive::DriveInterface & drive, 
        castor::mediachanger::MediaChangerFacade & mc,
        TapeServerReporter & tsr,
        MigrationWatchDog & mwd,
        const VolumeInfo& volInfo,
        cta::log::LogContext & lc,
        MigrationReportPacker & repPacker,
        cta::server::ProcessCap &capUtils,
        uint64_t filesBeforeFlush, uint64_t bytesBeforeFlush,
        const bool useLbp): 
        TapeSingleThreadInterface<TapeWriteTask>(drive, mc, tsr, volInfo,capUtils, lc),
        m_filesBeforeFlush(filesBeforeFlush),
        m_bytesBeforeFlush(bytesBeforeFlush),
        m_drive(drive),
        m_reportPacker(repPacker),
        m_lastFseq(-1),
        m_compress(true),
        m_useLbp(useLbp),
        m_watchdog(mwd){}
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
    ScopedParam(m_logContext, Param("TPVID",m_vid)),
    ScopedParam(m_logContext, Param("lastFseq", m_lastFseq)),
    ScopedParam(m_logContext, Param("compression", m_compress)),
    ScopedParam(m_logContext, Param("useLbp", m_useLbp))
  };
  tape::utils::suppresUnusedVariable(sp);
  try {
    writeSession.reset(
    new castor::tape::tapeFile::WriteSession(m_drive, m_volInfo, m_lastFseq,
      m_compress, m_useLbp)
    );
    m_logContext.log(cta::log::INFO, "Tape Write session session successfully started");
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
  

  m_reportPacker.reportFlush(m_drive.getCompression());
  m_drive.clearCompressionStats();
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
  mountTypeToString(const cta::MountType::Enum mountType) const throw() {
  switch(mountType) {
  case cta::MountType::RETRIEVE: return "RETRIEVE";
  case cta::MountType::ARCHIVE : return "ARCHIVE";
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
  std::string currentErrorToCount = "Error_setCapabilities";
  try
  {
    // Set capabilities allowing rawio (and hence arbitrary SCSI commands)
    // through the st driver file descriptor.
    setCapabilities();
    
    // Report the parameters of the session to the main thread
    typedef cta::log::Param Param;
    m_watchdog.addParameter(Param("TPVID", m_volInfo.vid));
    m_watchdog.addParameter(Param("mountType", mountTypeToString(m_volInfo.mountType)));
    
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
      currentErrorToCount = "Error_tapeMountForWrite";
      m_reportPacker.reportDriveStatus(cta::common::DriveStatus::Mounting);
      // Before anything, the tape should be mounted
      // This call does the logging of the mount
      mountTapeReadWrite();
      currentErrorToCount = "Error_tapeLoad";
      waitForDrive();
      currentErrorToCount = "Error_checkingTapeAlert";
      if (logTapeAlerts()) {
        throw cta::exception::Exception("Aborting migration session in presence of tape alerts");
      }
      currentErrorToCount = "Error_tapeNotWriteable";
      isTapeWritable();
      
      m_stats.mountTime += timer.secs(cta::utils::Timer::resetCounter);
      {
        cta::log::ScopedParamContainer scoped(m_logContext);
        scoped.add("mountTime", m_stats.mountTime);
        m_logContext.log(cta::log::INFO, "Tape mounted and drive ready");
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
      m_reportPacker.reportDriveStatus(cta::common::DriveStatus::Transfering); 
      while(1) {
        //get a task
        task.reset(m_tasks.pop());
        m_stats.waitInstructionsTime += timer.secs(cta::utils::Timer::resetCounter);
        //if is the end
        if(NULL==task.get()) {      
          //we flush without asking
          tapeFlush("No more data to write on tape, unconditional flushing to the client",bytes,files,timer);
          m_stats.flushTime += timer.secs(cta::utils::Timer::resetCounter);
          //end of session + log
          m_reportPacker.reportEndOfSession();
          cta::log::LogContext::ScopedParam sp0(m_logContext, cta::log::Param("tapeThreadDuration", totalTimer.secs()));
          m_logContext.log(cta::log::DEBUG, "writing data to tape has finished");
          break;
        }
        task->execute(*writeSession,m_reportPacker,m_watchdog,m_logContext,timer);
        // Add the tasks counts to the session's
        m_stats.add(task->getTaskStats());
        // Transmit the statistics to the watchdog thread
        m_watchdog.updateStats(m_stats);
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
    logWithStats(cta::log::INFO, "Tape thread complete",params);
    // Report one last time the stats, after unloading/unmounting.
    m_watchdog.updateStats(m_stats);
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
    m_watchdog.updateStats(m_stats);
    
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
      m_reportPacker.reportTapeFull();
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
          .add("ErrorMesage", errorMessage);
    m_stats.totalTime = totalTimer.secs();
    logWithStats(cta::log::INFO, "Tape thread complete",
            params);
    m_reportPacker.reportEndOfSessionWithErrors(errorMessage,errorCode);
  }      
}

//------------------------------------------------------------------------------
//logWithStats
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeWriteSingleThread::logWithStats(
int level,const std::string& msg, cta::log::ScopedParamContainer& params){
  params.add("type", "write")
        .add("TPVID", m_volInfo.vid)
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
        .add("transferTime", m_stats.transferTime())
        .add("totalTime", m_stats.totalTime)
        .add("dataVolume", m_stats.dataVolume)
        .add("headerVolume", m_stats.headerVolume)
        .add("files", m_stats.filesCount)
        .add("payloadTransferSpeedMBps", m_stats.totalTime?1.0*m_stats.dataVolume
                /1000/1000/m_stats.totalTime:0.0)
        .add("driveTransferSpeedMBps", m_stats.totalTime?1.0*(m_stats.dataVolume+m_stats.headerVolume)
                /1000/1000/m_stats.totalTime:0.0);
  m_logContext.log(level, msg);
}
