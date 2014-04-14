/******************************************************************************
 *                      MigrationTaskInjector.hpp
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

#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/log/LogContext.hpp"

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {

/**
 * This classis responsible for creating the tasks in case of a recall job
 */
class MigrationTaskInjector: public TaskInjector {  
public:

  MigrationTaskInjector(MemoryManager & mm, 
        DiskThreadPoolInterface<DiskReadTaskInterface> & diskReader,
        TapeSingleThreadInterface<TapeWriteTask> & tapeWriter,client::ClientInterface& client,
        castor::log::LogContext lc);

 
  /**
   * Create all the tape-read and write-disk tasks for set of files to retrieve
   * @param jobs
   */
  void injectBulkMigrations(const std::vector<castor::tape::tapegateway::FileToMigrateStruct*>& jobs);
  
  /**
   * Wait for the inner thread to finish
   */
  void waitThreads();
  
  /**
   * Start the inner thread 
   */
  void startThreads();
private:
 
  class WorkerThread: public castor::tape::threading::Thread {
  public:
    WorkerThread(MigrationTaskInjector & rji): _this(rji) {}
    virtual void run();
  private:
    MigrationTaskInjector & _this;
  } m_thread;

  MemoryManager & m_memManager;
  

  TapeSingleThreadInterface<TapeWriteTask>& m_tapeWriter;
  DiskThreadPoolInterface<DiskReadTaskInterface>& m_diskReader;
  client::ClientInterface& m_client;
  
  /**
   * utility member to log some pieces of information
   */
  castor::log::LogContext m_lc;
  
  castor::tape::threading::Mutex m_producerProtection;
  castor::tape::threading::BlockingQueue<Request> m_queue;
};

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor

