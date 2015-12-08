/******************************************************************************
 *                      TapeWriteSingleThread.hpp
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

#pragma once

#include "castor/server/ProcessCap.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/server/BlockingQueue.hpp"
#include "castor/server/Threading.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/utils/Timer.hpp"

#include <iostream>
#include <stdio.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {
// forward definition
class MigrationTaskInjector;
  
class TapeWriteSingleThread : public TapeSingleThreadInterface<TapeWriteTask> {
public:
  /**
   * Constructor
   * @param drive an interface for manipulating the drive in order 
   * to write on the tape 
   * @param vid the volume ID of the tape on which we are going to write
   * @param lc 
   * @param repPacker the object that will send reports to the client
   * @param filesBeforeFlush  how many file written before flushing on tape
   * @param bytesBeforeFlush how many bytes written before flushing on tape
   * @param lastFseq the last fSeq 
   */
  TapeWriteSingleThread(
    castor::tape::tapeserver::drive::DriveInterface & drive, 
    mediachanger::MediaChangerFacade &mc,
    TapeServerReporter & tsr,
    MigrationWatchDog & mwd,
    const VolumeInfo& volInfo,
    castor::log::LogContext & lc,
    MigrationReportPacker & repPacker,
    castor::server::ProcessCap &capUtils,
    uint64_t filesBeforeFlush, uint64_t bytesBeforeFlush);
    
  /**
   * 
   * @param lastFseq
   */
  void setlastFseq(uint64_t lastFseq);
  
  /**
   * Sets up the pointer to the task injector. This cannot be done at
   * construction time as both task injector and tape write single thread refer to 
   * each other. This function should be called before starting the threads.
   * This is used for signalling problems during mounting. After that, each 
   * tape write task does the signalling itself, either on tape problem, or
   * when receiving an error from the disk tasks via memory blocks.
   * @param injector pointer to the task injector
   */
  void setTaskInjector(MigrationTaskInjector* injector){
      m_injector = injector;
  }
private:  
  
  /**
   * Returns the string representation of the specified mount type
   */
  const char *mountTypeToString(const cta::MountType::Enum mountType) const
    throw();
  
    class TapeCleaning{
    TapeWriteSingleThread& m_this;
    // As we are living in the single thread of tape, we can borrow the timer
    castor::utils::Timer & m_timer;
  public:
    TapeCleaning(TapeWriteSingleThread& parent, castor::utils::Timer & timer):
      m_this(parent), m_timer(timer) {}
    ~TapeCleaning(){
      m_this.m_reportPacker.reportDriveStatus(cta::DriveStatus::CleaningUp);
      // This out-of-try-catch variables allows us to record the stage of the 
      // process we're in, and to count the error if it occurs.
      // We will not record errors for an empty string. This will allow us to
      // prevent counting where error happened upstream.
      // Log (safely, exception-wise) the tape alerts (if any) at the end of the session
      try { m_this.logTapeAlerts(); } catch (...) {}
      std::string currentErrorToCount = "Error_tapeUnload";
      try{
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
          m_this.m_reportPacker.reportDriveStatus(cta::DriveStatus::Unloading);
          m_this.m_drive.unloadTape();
          m_this.m_logContext.log(LOG_INFO, "TapeWriteSingleThread: Tape unloaded");
        } else {
          m_this.m_logContext.log(LOG_INFO, "TapeWriteSingleThread: Tape NOT unloaded (manual mode)");
        }
        m_this.m_stats.unloadTime += m_timer.secs(castor::utils::Timer::resetCounter);
        // And return the tape to the library
        // In case of manual mode, this will be filtered by the rmc daemon
        // (which will do nothing)
        currentErrorToCount = "Error_tapeDismount";
        m_this.m_reportPacker.reportDriveStatus(cta::DriveStatus::Unmounting);
        m_this.m_mc.dismountTape(m_this.m_volInfo.vid, m_this.m_drive.config.getLibrarySlot());
        m_this.m_reportPacker.reportDriveStatus(cta::DriveStatus::Up);
        m_this.m_stats.unmountTime += m_timer.secs(castor::utils::Timer::resetCounter);
        m_this.m_logContext.log(LOG_INFO, mediachanger::TAPE_LIBRARY_TYPE_MANUAL != m_this.m_drive.config.getLibrarySlot().getLibraryType() ?
          "TapeWriteSingleThread : tape unmounted":"TapeWriteSingleThread : tape NOT unmounted (manual mode)");
        m_this.m_initialProcess.tapeUnmounted();
        m_this.m_stats.waitReportingTime += m_timer.secs(castor::utils::Timer::resetCounter);
      }
      catch(const castor::exception::Exception& ex){
        // Notify something failed during the cleaning 
        m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
        m_this.m_reportPacker.reportDriveStatus(cta::DriveStatus::Down);
        castor::log::ScopedParamContainer scoped(m_this.m_logContext);
        scoped.add("exception_message", ex.getMessageValue())
        .add("exception_code",ex.code());
        m_this.m_logContext.log(LOG_ERR, "Exception in TapeWriteSingleThread-TapeCleaning");
        // As we do not throw exceptions from here, the watchdog signalling has
        // to occur from here.
        try {
          if (currentErrorToCount.size()) {
            m_this.m_watchdog.addToErrorCount(currentErrorToCount);
          }
        } catch (...) {}
      } catch (...) {
          // Notify something failed during the cleaning 
          m_this.m_hardwareStatus = Session::MARK_DRIVE_AS_DOWN;
          m_this.m_reportPacker.reportDriveStatus(cta::DriveStatus::Down);
          m_this.m_logContext.log(LOG_ERR, "Non-Castor exception in TapeWriteSingleThread-TapeCleaning when unmounting the tape");
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
  };
  /**
   * Will throw an exception if we cant write on the tape
   */
  void isTapeWritable() const;

  /**
   * Log  m_stats  parameters into m_logContext with msg at the given level
   */
  void logWithStats(int level,const std::string& msg,
    log::ScopedParamContainer& params);
  
  /**
   * Function to open the WriteSession 
   * If successful, returns a std::unique_ptr on it. A copy of that std::unique_ptr
   * will give the caller the ownership of the opened session (see unique_ptr 
   * copy constructor, which has a move semantic)
   * @return the WriteSession we need to write on tape
   */
  std::unique_ptr<castor::tape::tapeFile::WriteSession> openWriteSession();
  /**
   * Execute flush on tape, do some log and report the flush to the client
   * @param message the message the log will register
   * @param bytes the number of bytes that have been written since the last flush  
   * (for logging)
   * @param files the number of files that have been written since the last flush  
   * (also for logging)
   */
  void tapeFlush(const std::string& message,uint64_t bytes,uint64_t files,
    castor::utils::Timer & timer);
  

  virtual void run() ;
  
  //m_filesBeforeFlush and m_bytesBeforeFlush are thresholds for flushing 
  //the first one crossed will trigger the flush on tape
  
  ///how many file written before flushing on tape
  const uint64_t m_filesBeforeFlush;
  
  ///how many bytes written before flushing on tape
  const uint64_t m_bytesBeforeFlush;

  ///an interface for manipulating all type of drives
  castor::tape::tapeserver::drive::DriveInterface& m_drive;
  
  ///the object that will send reports to the client
  MigrationReportPacker & m_reportPacker;
  
  /**
   * the last fseq that has been written on the tape = the starting point 
   * of our session. The last Fseq is computed by subtracting 1 to fSeg
   * of the first file to migrate we receive. That part is done by the 
   * MigrationTaskInjector.::synchronousInjection. Thus, we compute it into 
   * that function and retrieve/set it within DataTransferSession executeWrite
   * after we make sure synchronousInjection returned true. 
   * 
   * It should be const, but it cant 
   * (because there is no mutable function member in c++)
   */
   uint64_t m_lastFseq;

  /**
   * Should the compression be enabled ? This is currently hard coded to true 
   */
  const bool m_compress;
  
  /**
   * Reference to the watchdog, used in run()
   */
  MigrationWatchDog & m_watchdog;
  
protected:
  /***
   * Helper virtual function to access the watchdog from parent class
   */
  virtual void countTapeLogError(const std::string & error) { 
    m_watchdog.addToErrorCount(error);
  }
  
private:
  /**
   *  Pointer to the task injector allowing termination signaling 
   */
  MigrationTaskInjector* m_injector;

}; // class TapeWriteSingleThread

} // namespace daemon
} // namespace tapeserver
} // namsepace tape
} // namespace castor
