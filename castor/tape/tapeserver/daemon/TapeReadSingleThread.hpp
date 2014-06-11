/******************************************************************************
 *                      TapeReadSingleThread.hpp
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
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/utils/Timer.hpp"
#include <iostream>
#include <stdio.h>
#include <memory>
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/GlobalStatusReporter.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/tape/tapeserver/daemon/CapabilityUtils.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//forward declaration
class GlobalStatusReporter;

  /**
   * This class will execute the different tape read tasks.
   * 
   */
class TapeReadSingleThread : public TapeSingleThreadInterface<TapeReadTask>{
public:
  /**
   * Constructor 
   * @param drive The drive which holds all we need in order to read later data from it
   * @param vid Volume ID (tape number)
   * @param maxFilesRequest : the maximul number of file the task injector may 
   * ask to the client in a single requiest, this is used for the feedback loop
   * @param lc : log context, for logging purpose
   */
  TapeReadSingleThread(castor::tape::drives::DriveInterface & drive,
          castor::legacymsg::RmcProxy & rmc,
          GlobalStatusReporter & gsr,
          const client::ClientInterface::VolumeInfo& volInfo, uint64_t maxFilesRequest,
          CapabilityUtils &capUtils,castor::log::LogContext & lc): 
   TapeSingleThreadInterface<TapeReadTask>(drive, rmc, gsr,volInfo,capUtils,lc),
   m_maxFilesRequest(maxFilesRequest) {
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
  public:
    TapeCleaning(TapeReadSingleThread& parent):m_this(parent){}
    ~TapeCleaning(){
        // Tell everyone to wrap up the session
        // We now acknowledge to the task injector that read reached the end. There
        // will hence be no more requests for more.
        m_this.m_taskInjector->finish();
        //then we log/notify
        m_this.m_logContext.log(LOG_INFO, "Finishing Tape Read Thread."
        " Just signaled task injector of the end");
        try {
          tape::utils::Timer timer;
          // Do the final cleanup
          m_this.m_drive.unloadTape();
          m_this.m_logContext.log(LOG_INFO, "TapeReadSingleThread : Tape unloaded");
          // And return the tape to the library
          m_this.m_rmc.unmountTape(m_this.m_volInfo.vid, m_this.m_drive.librarySlot);
          
          log::LogContext::ScopedParam sp0( m_this.m_logContext, log::Param("time taken", timer.usecs()));
          m_this.m_logContext.log(LOG_INFO, "TapeReadSingleThread : tape unmounted");
          m_this.m_gsr.tapeUnmounted();
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
        m_this.m_gsr.finish();
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

    try{
      
      setCapabilities();
      
      // the tape ins loaded 
      //it has to be unloaded, unmounted at all cost -> RAII
      //will also take care of the GlobalStatusReporer and of RecallTaskInjector
      TapeCleaning tapeCleaner(*this);
      
      // Before anything, the tape should be mounted
      mountTape(legacymsg::RmcProxy::MOUNT_MODE_READONLY);
      waitForDrive();
      
      // Then we have to initialise the tape read session
      std::auto_ptr<castor::tape::tapeFile::ReadSession> rs(openReadSession());
      
      //and then report
      m_logContext.log(LOG_INFO, "Tape read session session successfully started");
      m_gsr.tapeMountedForRead();
      tape::utils::Timer timer;
      // Then we will loop on the tasks as they get from 
      // the task injector
      while(1) {
        // NULL indicated the end of work
        TapeReadTask * task = popAndRequestMoreJobs();
        m_logContext.log(LOG_DEBUG, "TapeReadThread: just got one more job");
        if (task) {
          task->execute(*rs, m_logContext);
          delete task;
        } else {
          log::LogContext::ScopedParam sp0(m_logContext, log::Param("time taken", timer.secs()));
          m_logContext.log(LOG_INFO, "reading data from the tape has finished");
          break;
        }
      }
    } catch(const castor::exception::Exception& e){
      // we can only end there because 
      // moundTape, waitForDrive or crating the ReadSession failed
      // that means we cant do anything because the environment is wrong 
      // so we have to delete all task and return

      m_logContext.log(LOG_ERR, "Tape read session failed to start");
      while(1){
        TapeReadTask* task=m_tasks.pop();
        if(!task) {
          break;
        }
        task->reportErrorToDiskTask();
        delete task;
      }
    }
  }
  
  /**
   * Number of files a single request to the client might give us.
   * Used in the loop-back function to ask the task injector to request more job
   */
  const uint64_t m_maxFilesRequest;
  
  ///a pointer to task injector, thus we can ask him for more tasks
  castor::tape::tapeserver::daemon::RecallTaskInjector * m_taskInjector;
  
  
};
}
}
}
}

