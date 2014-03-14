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

#include <list>
#include <stdint.h>

#include "MemManager.hpp"
#include "TapeReadSingleThread.hpp"
#include "TapeReadFileTask.hpp"
#include "DiskWriteThreadPool.hpp"
#include "DiskWriteFileTask.hpp"
#include "RecallJob.hpp"
#include "TapeWriteFileTask.hpp"
#include "ClientInterface.hpp"
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

  RecallTaskInjector(MemoryManager & mm, 
        TapeReadSingleThread & tapeReader,
        DiskWriteThreadPool & diskWriter,ClientInterface& client,
        castor::log::LogContext& lc);

  
  /**
   * Function for a feed-back loop purpose between RecallTaskInjector and 
   * DiskWriteThreadPool. When DiskWriteThreadPool::popAndRequestMoreJobs detects 
   * it has not enough jobs to do to, it is class to push a request 
   * in order to (try) fill up the queue. 
   * @param maxFiles  files count requested.
   * @param maxBlocks total bytes count at least requested
   * @param lastCall true if we want the new request to be a last call. 
   * See Request::lastCall 
   */
  virtual void requestInjection(int maxFiles, int byteSizeThreshold, bool lastCall);

    /**
     * Contact the client to make sure there are really something to do
     * Something = recall at most  maxFiles or at least maxBytes
     * 
     * @param maxFiles files count requested.
     * @param byteSizeThreshold total bytes count  at least requested
     * @return true if there are jobs to be done, false otherwise 
     */
  bool synchronousInjection(uint64_t maxFiles, uint64_t byteSizeThreshold);

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
  void injectBulkRecalls(const std::list<RecallJob>& jobs);

  /**
   * A request of files to recall. We request EITHER
   * - a maximum of nbMaxFiles files
   * - OR at least byteSizeThreshold bytes. 
   * That means we stop as soon as we have nbMaxFiles files or the cumulated size 
   * is equal or above byteSizeThreshold. 
   */
  class Request {
  public:
    Request(int mf, int mb, bool lc):
    nbMaxFiles(mf), byteSizeThreshold(mb), lastCall(lc) {}
    
    const int nbMaxFiles;
    const int byteSizeThreshold;
    
    /** 
     * True if it is the last call for the set of requests :it means
     *  we don't need to try to get more files to recall 
     *  and can send into all the different threads a signal  .
     */
    const bool lastCall;
  };
  
  class WorkerThread: public castor::tape::threading::Thread {
  public:
    WorkerThread(RecallTaskInjector & rji): _this(rji) {}
    void run();
  private:
    RecallTaskInjector & _this;

  } m_thread;

  MemoryManager & m_memManager;
  

  TapeReadSingleThread & m_tapeReader;
  DiskWriteThreadPool & m_diskWriter;
  ClientInterface& m_client;
  
  /**
   * utility member to log some pieces of information
   */
  castor::log::LogContext& m_lc;
  
  castor::tape::threading::Mutex m_producerProtection;
  castor::tape::threading::BlockingQueue<Request> m_queue;
};

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor
