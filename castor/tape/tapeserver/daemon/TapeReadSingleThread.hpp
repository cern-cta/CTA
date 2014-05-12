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
#include "castor/legacymsg/RmcProxy.hpp"
#include <iostream>
#include <stdio.h>
#include <memory>
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"

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
          const std::string vid, uint64_t maxFilesRequest,
          castor::log::LogContext & lc): 
   TapeSingleThreadInterface<TapeReadTask>(drive, rmc, gsr, vid, lc),
   m_maxFilesRequest(maxFilesRequest),m_filesProcessed(0) {}
   
   /**
    * Set the task injector. Has to be done that way (and not in the constructor)
    *  because there is a dependency 
    * @param ti the task injector 
    */
   void setTaskInjector(RecallTaskInjector * ti) { 
     m_taskInjector = ti; 
   }

private:
  
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
   * This function is from Thread, it is the function that will do all the job
   */
  virtual void run() {
    m_logContext.pushOrReplace(log::Param("thread", "tapeRead"));
    // Before anything, the tape should be mounted
    m_rmc.mountTape(m_vid, m_drive.librarySlot);
    // Then we have to initialise the tape read session
    std::auto_ptr<castor::tape::tapeFile::ReadSession> rs;
    try {
      rs.reset(new castor::tape::tapeFile::ReadSession(m_drive, m_vid));
      m_logContext.log(LOG_INFO, "Tape read session session successfully started");
    } catch (castor::exception::Exception & ex) {
      m_logContext.log(LOG_ERR, "Failed to start tape read session");
      // TODO: log and unroll the session
      // TODO: add an unroll mode to the tape read task. (Similar to exec, but pushing blocks marked in error)
    }
    // Then we will loop on the tasks as they get from 
    // the task injector
    while(1) {
      // NULL indicated the end of work
      TapeReadTask * task = popAndRequestMoreJobs();
      m_logContext.log(LOG_DEBUG, "TapeReadThread: just got one more job");
      if (task) {
        task->execute(*rs, m_logContext);
        delete task;
        m_filesProcessed++;
      } else {
        break;
      }
    }
    // We now acknowledge to the task injector that read reached the end. There
    // will hence be no more requests for more. (last thread turns off the light)
    m_taskInjector->finish();
    // Do the final cleanup
    m_drive.unloadTape();
    // And return the tape to the library
    m_rmc.unmountTape(m_vid, m_drive.librarySlot);
    m_logContext.log(LOG_DEBUG, "Finishing Tape Read Thread. Just signalled task injector of the end");
  }
  
  /**
   * Number of files a single request to the client might give us.
   * Used in the loop-back function to ask the task injector to request more job
   */
  const uint64_t m_maxFilesRequest;
  
  ///a pointer to task injector, thus we can ask him for more tasks
  castor::tape::tapeserver::daemon::RecallTaskInjector * m_taskInjector;
  
  ///how many files have we already processed(for loopback purpose)
  size_t m_filesProcessed;
};
}
}
}
}

