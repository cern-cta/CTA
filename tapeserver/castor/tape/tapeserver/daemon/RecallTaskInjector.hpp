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

#include <stdint.h>
#include "common/log/LogContext.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/threading/Thread.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

#include <future>

namespace castor{
namespace tape{
  //forward declarations
  namespace tapegateway {
    class FileToRecallStruct;
  }
namespace tapeserver{
  namespace client {
    class ClientInterface;
  }
namespace daemon {

  //forward declaration
  class RecallMemoryManager;
  class DiskWriteThreadPool;
  class TapeReadTask;
  //forward declaration of template class
  template <class T> class TapeSingleThreadInterface;
  
/**
 * This classis responsible for creating the tasks in case of a recall job
 */
class RecallTaskInjector {  
public:
 /**
  * Constructor
  * @param mm the memory manager from whom the TRT will be pulling blocks
  * @param tapeReader the one object that will hold the thread which will be executing 
  * tape-reading tasks
  * @param diskWriter the one object that will hold all the threads which will be executing 
  * disk-writing tasks
  * @param client The one that will give us files to recall 
  * @param filesPerRequest number of files we request from the client at once 
  * @param bytesPerRequest number of bytes we request from the client at once 
  * we may request to the client. at once
  * @param lc  copied because of the threading mechanism 
  */
  RecallTaskInjector(RecallMemoryManager & mm, 
        TapeSingleThreadInterface<TapeReadTask> & tapeReader,
        DiskWriteThreadPool & diskWriter,cta::RetrieveMount &retrieveMount,
        uint64_t filesPerRequest, uint64_t bytesPerRequest,cta::log::LogContext lc);

  virtual ~RecallTaskInjector();
  
  /**
   * Function for a feed-back loop purpose between RecallTaskInjector and 
   * TapeReadSingleThread. When TapeReadSingleThread::popAndRequestMoreJobs detects 
   * it has not enough jobs to do to, it will push a request, that when executed 
   * will ask the client to try to fill up the queue. 

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
  bool synchronousFetch();

  /**
   * Wait for the inner thread to finish
   */
  void waitThreads();
  
  /**
   * Start the inner thread 
   */
  void startThreads();

  /**
   * Set the drive interface in use
   * @param di - Drive interface
   */
  void setDriveInterface(castor::tape::tapeserver::drive::DriveInterface *di);

  /**
   * Initialize Recommended Access Order parameters
   */
  void initRAO();

  bool waitForPromise();

  void setPromise();

private:
  /**
   * It will signal to the disk read thread  pool, tape write single thread
   * and to the mem manager they have to stop their threads(s)
   */
  void signalEndDataMovement();

  /**
   * It will delete all remaining tasks 
   */
  void deleteAllTasks();
  
  /**
   * Create all the tape-read and write-disk tasks for set of files to retrieve
   */
  void injectBulkRecalls();

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
    filesRequested(mf), bytesRequested(mb), lastCall(lc),end(false) {}
    
    Request():
    filesRequested(0), bytesRequested(0), lastCall(true),end(true) {}
    const uint64_t filesRequested;
    const uint64_t bytesRequested;
    
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
  
  class WorkerThread: public cta::threading::Thread {
  public:
    WorkerThread(RecallTaskInjector & rji): m_parent(rji) {}
    virtual void run();
  private:
    RecallTaskInjector & m_parent;
  } m_thread;
  ///The memory manager for accessing memory blocks. 
  RecallMemoryManager & m_memManager;
  
  ///the one object that will hold the thread which will be executing 
  ///tape-reading tasks
  TapeSingleThreadInterface<TapeReadTask> & m_tapeReader;
  
  ///the one object that will hold all the threads which will be executing 
  ///disk-writing tasks
  DiskWriteThreadPool & m_diskWriter;
  
  /// the client who is sending us jobs
  cta::RetrieveMount &m_retrieveMount;
  
  /// Drive interface needed for performing Recommended Access Order query
  castor::tape::tapeserver::drive::DriveInterface * m_drive;

  std::vector<std::unique_ptr<cta::RetrieveJob>> m_jobs;

  /**
   * utility member to log some pieces of information
   */
  cta::log::LogContext m_lc;
  
  cta::threading::Mutex m_producerProtection;
  cta::threading::BlockingQueue<Request> m_queue;
  

  //maximal number of files requested. at once
  const uint64_t m_maxFiles;
  
  //maximal number of cumulated byte requested. at once
  const uint64_t m_maxBytes;

  /** Flag indicating if the file recalls are performed using
   * the Recommended Access Order (RAO)
   */
  bool m_useRAO;

  /** Drive-specific RAO parameters */
  SCSI::Structures::RAO::udsLimits m_raoLimits;

  /**
   * The promise for reordering the read tasks according to RAO by the
   * RecallTaskInjector. The tasks to be run are placed in the m_tasks queue
   */
  std::promise<void> m_raoPromise;
  std::future<void> m_raoFuture;
};

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor
