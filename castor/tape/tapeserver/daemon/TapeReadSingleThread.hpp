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
#include <iostream>
#include <stdio.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
class TapeReadSingleThread : public TapeSingleThreadInterface<TapeReadTask>{
public:
  TapeReadSingleThread(castor::tape::drives::DriveInterface & drive,
          const std::string vid, uint64_t maxFilesRequest,
          castor::log::LogContext & lc): 
   TapeSingleThreadInterface<TapeReadTask>(drive, vid, lc) {}
   void setTaskInjector(TaskInjector * ti) { m_taskInjector = ti; }

private:
  TapeReadTask * popAndRequestMoreJobs() {
    castor::tape::threading::BlockingQueue<TapeReadTask *>::valueRemainingPair 
      vrp = m_tasks.popGetSize();
    // If we just passed (down) the half full limit, ask for more
    // (the remaining value is after pop)
    if(vrp.remaining + 1 == m_maxFilesRequest/2) {
      // This is not a last call
      m_taskInjector->requestInjection(m_maxFilesRequest, 1000, false);
    } else if (0 == vrp.remaining) {
      // This is a last call: if the task injector comes up empty on this
      // one, he'll call it the end.
      m_taskInjector->requestInjection(m_maxFilesRequest, 1000, false);
    }
    return vrp.value;
  }
  
  
  
  virtual void run() {
    // First we have to initialise the tape read session
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
      TapeReadTask * task = popAndRequestMoreJobs();
      bool end = task->endOfWork();
      if (!end) task->execute(*rs, m_logContext);
      delete task;
      m_filesProcessed++;
      if (end) {
        return;
      }
    }
  }
  
  uint64_t m_maxFilesRequest;
};
}
}
}
}

