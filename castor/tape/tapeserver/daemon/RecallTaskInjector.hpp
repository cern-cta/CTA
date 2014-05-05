/******************************************************************************
 *                      RecallTaskInjector.hpp
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

#include <stdint.h>

#include <list>
#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/log/LogContext.hpp"
namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {

/**
 * This classis responsible for creating the tasks in case of a recall job
 */
class RecallTaskInjector: public TaskInjector {  
public:
 /**
  * Constructor
  * @param mm the memory manager from whom the TRT will be pulling blocks
  * @param tapeReader the one object that will hold the thread which will be executing 
  * tape-reading tasks
  * @param diskWriter the one object that will hold all the threads which will be executing 
  * disk-writing tasks
  * @param client The one that will give us files to recall 
  * @param maxFiles maximal number of files we may request to the client at once 
  * @param byteSizeThreshold maximal number of cumulated byte 
  * we may request to the client. at once
  * @param lc  copied because of the threading mechanism 
  */
  RecallTaskInjector(RecallMemoryManager & mm, 
        TapeSingleThreadInterface<TapeReadTaskInterface> & tapeReader,
        DiskWriteThreadPool & diskWriter,client::ClientInterface& client,
        uint64_t maxFiles, uint64_t byteSizeThreshold,castor::log::LogContext lc);

  
  /**
   * Function for a feed-back loop purpose between RecallTaskInjector and 
   * TapeReadSingleThread. When TapeReadSingleThread::popAndRequestMoreJobs detects 
   * it has not enough jobs to do to, it is class to push a request 
   * in order to (try) fill up the queue. 

   * @param lastCall true if we want the new request to be a last call. 
   * See Request::lastCall 
   */
  virtual void requestInjection(bool lastCall);

  /**
   * Send an end token in the request queue. There should be no subsequent
   * calls to requestInjection.
   */
  void finish();
  
    /**
     * Contact the client to make sure there are really something to do
     * Something = recall at most  maxFiles or at least maxBytes
     * 
     * @param maxFiles files count requested.
     * @param byteSizeThreshold total bytes count  at least requested
     * @return true if there are jobs to be done, false otherwise 
     */
  bool synchronousInjection();

  /**
   * Wait for the inner thread to finish
   */
  void waitThreads();
  
  /**
   * Start the inner thread 
   */
  void startThreads();
private:
  
  /**
   * Create all the tape-read and write-disk tasks for set of files to retrieve
   * @param jobs
   */
  void injectBulkRecalls(const std::vector<castor::tape::tapegateway::FileToRecallStruct*>& jobs);

  /**
   * A request of files to recall. We request EITHER
   * - a maximum of nbMaxFiles files
   * - OR at least byteSizeThreshold bytes. 
   * That means we stop as soon as we have nbMaxFiles files or the cumulated size 
   * is equal or above byteSizeThreshold. 
   */
  class Request {
  public:
    Request(uint64_t mf, uint64_t mb, bool lc):
    nbMaxFiles(mf), byteSizeThreshold(mb), lastCall(lc),end(false) {}
    
    Request():
    nbMaxFiles(0), byteSizeThreshold(0), lastCall(true),end(true) {}
    const uint64_t nbMaxFiles;
    const uint64_t byteSizeThreshold;
    
    /** 
     * True if it is the last call for the set of requests :it means
     *  we don't need to try to get more files to recall 
     *  and can send into all the different threads a signal  .
     */
    const bool lastCall;
    
    /**
     * Set as true if the loop back process notified to us the end 
     */
    const bool end;
  };
  
  class WorkerThread: public castor::tape::threading::Thread {
  public:
    WorkerThread(RecallTaskInjector & rji): _this(rji) {}
    virtual void run();
  private:
    RecallTaskInjector & _this;
  } m_thread;
  ///The memory manager for accessing memory blocks. 
  RecallMemoryManager & m_memManager;
  
  ///the one object that will hold the thread which will be executing 
  ///tape-reading tasks
  TapeSingleThreadInterface<TapeReadTaskInterface> & m_tapeReader;
  
  ///the one object that will hold all the threads which will be executing 
  ///disk-writing tasks
  DiskWriteThreadPool & m_diskWriter;
  
  /// the client who is sending us jobs
  client::ClientInterface& m_client;
  
  /**
   * utility member to log some pieces of information
   */
  castor::log::LogContext m_lc;
  
  castor::tape::threading::Mutex m_producerProtection;
  castor::tape::threading::BlockingQueue<Request> m_queue;
  

  //maximal number of files requested. at once
  const uint64_t m_maxFiles;
  
  //maximal number of cumulated byte requested. at once
  const uint64_t m_byteSizeThreshold;
};

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor
