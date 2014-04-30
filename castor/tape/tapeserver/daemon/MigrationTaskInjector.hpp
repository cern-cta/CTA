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
#include "castor/tape/tapeserver/daemon/TaskInjector.hpp"
#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {

/**
 * This classis responsible for creating the tasks in case of a recall job
 */
class MigrationTaskInjector: public TaskInjector {  
public:

  MigrationTaskInjector(MigrationMemoryManager & mm, 
        DiskThreadPoolInterface<DiskReadTaskInterface> & diskReader,
        TapeSingleThreadInterface<TapeWriteTaskInterface> & tapeWriter,client::ClientInterface& client,
        uint64_t maxFiles, uint64_t byteSizeThreshold,castor::log::LogContext lc);

 
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
  /**
   * Function for a feed-back loop purpose between MigrationTaskInjector and 
   * DiskReadThreadPool. When DiskReadThreadPool::popAndRequestMoreJobs detects 
   * it has not enough jobs to do to, it is class to push a request 
   * in order to (try) fill up the queue. 
   * @param maxFiles  files count requested.
   * @param maxBlocks total bytes count at least requested
   * @param lastCall true if we want the new request to be a last call. 
   * See Request::lastCall 
   */
  void requestInjection(bool lastCall);
  
  /**
   * Contact the client to make sure there are really something to do
   * Something = migration at most  maxFiles or at least maxBytes
   * 
   * @param maxFiles files count requested.
   * @param byteSizeThreshold total bytes count  at least requested
   * @return true if there are jobs to be done, false otherwise 
   */
  bool synchronousInjection();
  
  /**
   * Send an end token in the request queue. There should be no subsequent
   * calls to requestInjection.
   */
  void finish();
private:
  
  /*Compute how many blocks are needed for a file of fileSize bytes*/
  size_t howManyBlocksNeeded(size_t fileSize,size_t blockCapacity){
    return fileSize/blockCapacity + ((fileSize%blockCapacity==0) ? 0 : 1); 
  }
  
   /**
   * A request of files to migrate. We request EITHER
   * - a maximum of nbMaxFiles files
   * - OR at least byteSizeThreshold bytes. 
   * That means we stop as soon as we have nbMaxFiles files or the cumulated size 
   * is equal or above byteSizeThreshold. 
   */
  class Request {
  public:
    Request(int mf, int mb, bool lc):
    nbMaxFiles(mf), byteSizeThreshold(mb), lastCall(lc), end(false) {}
    
    Request():
    nbMaxFiles(-1), byteSizeThreshold(-1), lastCall(true),end(true) {}
    
    const int nbMaxFiles;
    const int byteSizeThreshold;
    
    /** 
     * True if it is the last call for the set of requests :it means
     *  we don't need to try to get more files to recall 
     *  and can send into all the different threads a signal  .
     */
    const bool lastCall;
    
    /**
     * True indicates the task injector will not receive any more request.
     */
    const bool end;
  };
  
  class WorkerThread: public castor::tape::threading::Thread {
  public:
    WorkerThread(MigrationTaskInjector & rji): m_parent(rji) {}
    virtual void run();
  private:
    MigrationTaskInjector & m_parent;
  } m_thread;

  MigrationMemoryManager & m_memManager;
  

  TapeSingleThreadInterface<TapeWriteTaskInterface>& m_tapeWriter;
  DiskThreadPoolInterface<DiskReadTaskInterface>& m_diskReader;
  client::ClientInterface& m_client;
  
  /**
   * utility member to log some pieces of information
   */
  castor::log::LogContext m_lc;
  
  castor::tape::threading::Mutex m_producerProtection;
  castor::tape::threading::BlockingQueue<Request> m_queue;
  
  castor::tape::threading::AtomicFlag m_errorFlag;

  //maximal number of files requested. at once
  const uint64_t m_maxFiles;
  
  //maximal number of cumulated byte requested. at once
  const uint64_t m_maxByte;
};

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor

