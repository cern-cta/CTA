/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/ArchiveMount.hpp"

namespace castor::tape::tapeserver::daemon {

/**
 * This class is responsible for creating the tasks in case of a recall job
 */
class MigrationTaskInjector {  
public:

  /**
   * Constructor
   * @param mm The memory manager for accessing memory blocks. 
   * The Newly created tapeWriter Tasks will register themselves 
   * as a client to it. 
   * @param diskReader the one object that will hold all the threads which will be executing 
   * disk-reading tasks
   * @param tapeWriter the one object that will hold the thread which will be executing 
   * tape-writing tasks
   * @param client The one that will give us files to migrate 
   * @param maxFiles maximal number of files we may request to the client at once 
   * @param byteSizeThreshold maximal number of cumulated byte 
   * we may request to the client. at once
   * @param lc log context, copied because of the threading mechanism 
   */
  MigrationTaskInjector(MigrationMemoryManager & mm, 
        DiskReadThreadPool & diskReader,
        TapeSingleThreadInterface<TapeWriteTask> & tapeWriter,cta::ArchiveMount &archiveMount,
        uint64_t maxFiles, uint64_t byteSizeThreshold,const cta::log::LogContext& lc);

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
   * @param lastCall true if we want the new request to be a last call. 
   * See Request::lastCall 
   */
  void requestInjection(bool lastCall);
  
  /**
   * Contact the client to make sure there are really something to do
   * Something = migration at most  maxFiles or at least maxBytes
   * 
   * @param noFilesToMigrate[out] will be true if it triggered an empty mount because of no files to migrate
   * @return true if there are jobs to be done, false otherwise 
   */
  bool synchronousInjection(bool & noFilesToMigrate);
  
  /**
   * Send an end token in the request queue. There should be no subsequent
   * calls to requestInjection.
   */
  void finish();
  
  /**
   * Return the first file to be written's fseq
   * @return 
   */
  uint64_t firstFseqToWrite() const;
  
  /**
   * Public interface allowing to set the error flag. This is used
   * by the tasks (disk and tape) and the tape thread to indicate
   * that the session cannot continue.
   */
  void setErrorFlag() {
    m_errorFlag.set();
  }
  
  /**
   * Public interface to the error flag, allowing the disk and tape tasks
   * to decide whether they should carry on or just free memory.
   * @return value of the error flag
   */
  bool hasErrorFlag() {
    // AtomicFlag explicit conversion to bool prohibits copy-constructing the return value
    return m_errorFlag ? true : false;
  }
private:
  /**
   * Create all the tape-read and write-disk tasks for set of files to retrieve
   * @param jobs the list of FileToMigrateStructs we have to transform in a pair of task
   */
  void injectBulkMigrations(std::list<std::unique_ptr<cta::ArchiveJob>>& jobs);
  
  /*Compute how many blocks are needed for a file of fileSize bytes*/
  uint64_t howManyBlocksNeeded(uint64_t fileSize, size_t blockCapacity){
    const auto extraBlock = ((fileSize%blockCapacity) == 0) ? 0 : 1;
    return fileSize/blockCapacity + extraBlock;
  }
  
  /**
   * It will signal to the disk read thread  pool, tape write single thread
   * and to the mem manager they have to stop their threads(s)
   */
  void signalEndDataMovement();

   /**
   * A request of files to migrate. We request EITHER
   * - a maximum of nbMaxFiles files
   * - OR at least byteSizeThreshold bytes. 
   * That means we stop as soon as we have nbMaxFiles files or the cumulated size 
   * is equal or above byteSizeThreshold. 
   */
  class Request {
  public:
    Request(uint64_t mf, uint64_t mb, bool lc):
    filesRequested(mf), bytesRequested(mb), lastCall(lc), end(false) {}
    
    Request():
    filesRequested(0), bytesRequested(0), lastCall(true), end(true) {}
    
    const uint64_t filesRequested;
    const uint64_t bytesRequested;
    
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
  
  class WorkerThread : public cta::threading::Thread {
  public:
    explicit WorkerThread(MigrationTaskInjector& rji) : m_parent(rji) {}
    virtual void run();
  private:
    MigrationTaskInjector & m_parent;
  } m_thread;

  ///The memory manager for accessing memory blocks. 
  MigrationMemoryManager & m_memManager;
  
  ///the one object that will hold the thread which will be executing 
  ///tape-writing tasks
  TapeSingleThreadInterface<TapeWriteTask>& m_tapeWriter;
  
  ///the one object that will hold all the threads which will be executing 
  ///disk-reading tasks
  DiskReadThreadPool & m_diskReader;
  
  /// the client who is sending us jobs
  cta::ArchiveMount &m_archiveMount;
  
  /**
   * utility member to log some pieces of information
   */
  cta::log::LogContext m_lc;
  
  cta::threading::Mutex m_producerProtection;
  
  ///all the requests for work we will forward to the client.
  cta::threading::BlockingQueue<Request> m_queue;
  
  /** a shared flag among the all tasks related to migration, set as true 
   * as soon a single task encounters a failure. That way we go into a degraded mode
   * where we only circulate memory without writing anything on tape
   */ 
  cta::threading::AtomicFlag m_errorFlag;

  /// The maximum number of files we ask per request. 
  const uint64_t m_maxFiles;
  
  /// Same as m_maxFilesReq for size per request. (in bytes))
  const uint64_t m_maxBytes;
  
  /**The last fseq used on the tape. We should not see this but 
   * IT is computed by subtracting 1 to fSeg  of the first file to migrate we 
   * receive. That part is done by the 
   * MigrationTaskInjector.::synchronousInjection. Thus, we compute it into 
   * that function and retrieve/set it within DataTransferSession executeWrite
   * after we make sure synchronousInjection returned true. To do so, we
   *  need to store it
   */
  uint64_t m_firstFseqToWrite;
};

} // namespace castor::tape::tapeserver::daemon
