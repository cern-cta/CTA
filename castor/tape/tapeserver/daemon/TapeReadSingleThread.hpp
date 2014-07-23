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

#pragma once

#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/utils/Timer.hpp"

#include <iostream>
#include <memory>
#include <stdio.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//forward declaration
class TapeServerReporter;

  /**
   * This class will execute the different tape read tasks.
   * 
   */
class TapeReadSingleThread : public TapeSingleThreadInterface<TapeReadTask>{
public:
  /**
   * 
   */
  TapeReadSingleThread(castor::tape::tapeserver::drives::DriveInterface & drive,
          castor::legacymsg::RmcProxy & rmc,
          TapeServerReporter & initialProcess,
          const client::ClientInterface::VolumeInfo& volInfo, 
          uint64_t maxFilesRequest,
          castor::server::ProcessCap &capUtils,
          TaskWatchDog<detail::Recall>& watchdog,
          castor::log::LogContext & lc): 
   TapeSingleThreadInterface<TapeReadTask>(drive, rmc, initialProcess,volInfo,capUtils,lc),
   m_maxFilesRequest(maxFilesRequest),m_watchdog(watchdog) {
   }
   
   /**
    * Set the task injector. Has to be done that way (and not in the constructor)
    *  because there is a dep26569endency 
    * @param ti the task injector 
    */
   void setTaskInjector(RecallTaskInjector * ti) { 
     m_taskInjector = ti; 
   }

private:
  //RAII class for cleaning tape stuff
  class TapeCleaning{
    TapeReadSingleThread& m_this;
    // As we are living in the single thread of tape, we can borrow the timer
    utils::Timer & m_timer;
  public:
    TapeCleaning(TapeReadSingleThread& parent, utils::Timer & timer):
      m_this(parent), m_timer(timer){}
    ~TapeCleaning(){
        // Tell everyone to wrap up the session
        // We now acknowledge to the task injector that read reached the end. There
        // will hence be no more requests for more.
        m_this.m_taskInjector->finish();
        //then we log/notify
        m_this.m_logContext.log(LOG_DEBUG, "Starting session cleanup. Signaled end of session to task injector.");
        m_this.m_stats.waitReportingTime += m_timer.secs(utils::Timer::resetCounter);
        try {
          // Do the final cleanup
          // in the special case of a "manual" mode tape, we should skip the unload too.
          if (m_this.m_drive.librarySlot != "manual") {
            m_this.m_drive.unloadTape();
            m_this.m_logContext.log(LOG_INFO, "TapeReadSingleThread: Tape unloaded");
          } else {
            m_this.m_logContext.log(LOG_INFO, "TapeReadSingleThread: Tape NOT unloaded (manual mode)");
          }
          m_this.m_stats.unloadTime += m_timer.secs(utils::Timer::resetCounter);
          // And return the tape to the library
          // In case of manual mode, this will be filtered by the rmc daemon
          // (which will do nothing)
          m_this.m_rmc.unmountTape(m_this.m_volInfo.vid, m_this.m_drive.librarySlot);
          m_this.m_stats.unmountTime += m_timer.secs(utils::Timer::resetCounter);
          m_this.m_logContext.log(LOG_INFO, m_this.m_drive.librarySlot != "manual"?
            "TapeReadSingleThread : tape unmounted":"TapeReadSingleThread : tape NOT unmounted (manual mode)");
          m_this.m_initialProcess.tapeUnmounted();
          m_this.m_stats.waitReportingTime += m_timer.secs(utils::Timer::resetCounter);
        } catch(const castor::exception::Exception& ex){
          //set it to -1 to notify something failed during the cleaning 
          m_this.m_hardarwareStatus = -1;
          castor::log::ScopedParamContainer scoped(m_this.m_logContext);
          scoped.add("exception_message", ex.getMessageValue())
                .add("exception_code",ex.code());
          m_this.m_logContext.log(LOG_ERR, "Exception in TapeReadSingleThread-TapeCleaning when unmounting the tape");
        } catch (...) {
          //set it to -1 to notify something failed during the cleaning 
          m_this.m_hardarwareStatus = -1;
          m_this.m_logContext.log(LOG_ERR, "Non-Castor exception in TapeReadSingleThread-TapeCleaning when unmounting the tape");
        }
        //then we terminate the global status reporter
        m_this.m_initialProcess.finish();
    }
  };
  /**
   * Pop a task from its tasks and if there is not enough tasks left, it will 
   * ask the task injector for more 
   * @return m_tasks.pop();
   */
  TapeReadTask * popAndRequestMoreJobs() {
    castor::tape::threading::BlockingQueue<TapeReadTask *>::valueRemainingPair 
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
    
    /**
     * Try to open an tapeFile::ReadSession, if it fails, we got an exception.
     * Return an std::auto_ptr will ensure the callee will have the ownershipe 
     * of the object through auto_ptr's copy constructor
     * @return 
     */
  std::auto_ptr<castor::tape::tapeFile::ReadSession> openReadSession(){
    try{
      std::auto_ptr<castor::tape::tapeFile::ReadSession> rs(
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

  /**
   * This function is from Thread, it is the function that will do all the job
   */
  virtual void run() {
    m_logContext.pushOrReplace(log::Param("thread", "tapeRead"));
    castor::tape::utils::Timer timer, totalTimer;
    try{
      // Set capabilities allowing rawio (and hence arbitrary SCSI commands)
      // through the st driver file descriptor.
      setCapabilities();
      
      //pair of brackets to create an artificial scope for the tapeCleaner
      {  
        // The tape will be loaded 
        // it has to be unloaded, unmounted at all cost -> RAII
        // will also take care of the TapeServerReporter and of RecallTaskInjector
        TapeCleaning tapeCleaner(*this, timer);
        // Before anything, the tape should be mounted
        mountTape(legacymsg::RmcProxy::MOUNT_MODE_READONLY);
        waitForDrive();
        m_stats.mountTime += timer.secs(utils::Timer::resetCounter);
        {
          castor::log::ScopedParamContainer scoped(m_logContext);
          scoped.add("mountTime", m_stats.mountTime);
          m_logContext.log(LOG_INFO, "Tape mounted and drive ready");
        }
        // Then we have to initialise the tape read session
        std::auto_ptr<castor::tape::tapeFile::ReadSession> rs(openReadSession());
        m_stats.positionTime += timer.secs(utils::Timer::resetCounter);
        //and then report
        {
          castor::log::ScopedParamContainer scoped(m_logContext);
          scoped.add("positionTime", m_stats.positionTime);
          m_logContext.log(LOG_INFO, "Tape read session session successfully started");
        }
        m_initialProcess.tapeMountedForRead();
        m_stats.waitReportingTime += timer.secs(utils::Timer::resetCounter);
        // Then we will loop on the tasks as they get from 
        // the task injector
        std::auto_ptr<TapeReadTask> task;
        while(true) {
          //get a task
          task.reset(popAndRequestMoreJobs());
          m_stats.waitInstructionsTime += timer.secs(utils::Timer::resetCounter);
          // If we reached the end
          if (NULL==task.get()) {
            m_logContext.log(LOG_DEBUG, "No more files to read from tape");
            break;
          }
          // This can lead the the session being marked as corrupt, so we test
          // it in the while loop
          task->execute(*rs, m_logContext, m_watchdog,m_stats,timer);
          // The session could have been corrupted (failed positioning)
          if(rs->isCorrupted()) {
            throw castor::exception::Exception ("Session corrupted: exiting task execution loop in TapeReadSingleThread. Cleanup will follow.");
          }
        }
      }
      // The session completed successfully, and the cleaner (unmount) executed
      // at the end of the previous block. Log the results.
      log::ScopedParamContainer params(m_logContext);
      logWithStat(LOG_INFO, "Completed recall session's tape thread successfully",
              params,totalTimer.secs());
    } catch(const castor::exception::Exception& e){
      // We end up here because one step failed, be it at mount time, of after
      // failing to position by fseq (this is fatal to a read session as we need
      // to know where we are to proceed to the next file incrementally in fseq
      // positioning mode).
      // This can happen late in the session, so we can still print the stats.
      log::ScopedParamContainer params(m_logContext);
      params.add("ErrorMesage", e.getMessageValue());
      logWithStat(LOG_ERR, "Tape read thread had to be halted because of an error",
              params,totalTimer.secs());
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

  /**
   * Log msg with the given level, Session time is the time taken by the action 
   * @param level
   * @param msg
   * @param sessionTime
   */
  void logWithStat(int level,const std::string& msg,log::ScopedParamContainer& params,double sessionTime){
    params.add("mountTime", m_stats.mountTime)
          .add("positionTime", m_stats.positionTime)
          .add("waitInstructionsTime", m_stats.waitInstructionsTime)
          .add("checksumingTime", m_stats.checksumingTime)
          .add("transferTime", m_stats.transferTime)
          .add("waitFreeMemoryTime", m_stats.waitFreeMemoryTime)
          .add("waitReportingTime", m_stats.waitReportingTime)
          .add("unloadTime", m_stats.unloadTime)
          .add("unmountTime", m_stats.unmountTime)
          .add("dataVolume", m_stats.dataVolume)
          .add("dataBandwidthMiB/s", 1.0*m_stats.dataVolume
                                     /1024/1024/sessionTime)
          .add("driveBandwidthMiB/s", 1.0*(m_stats.dataVolume+m_stats.headerVolume)
                                     /1024/1024/sessionTime)
          .add("sessionTime", sessionTime);
          m_logContext.log(level,msg);
  }
  
  /**
   * Number of files a single request to the client might give us.
   * Used in the loop-back function to ask the task injector to request more job
   */
  const uint64_t m_maxFilesRequest;
  
  ///a pointer to task injector, thus we can ask him for more tasks
  castor::tape::tapeserver::daemon::RecallTaskInjector * m_taskInjector;
  
  TaskWatchDog<detail::Recall>& m_watchdog;

}; // class TapeReadSingleThread

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
