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

#include <stdint.h>
#include "common/log/LogContext.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/threading/Thread.hpp"
#include "common/dataStructures/DiskSpaceReservationRequest.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/RAO/RAOParams.hpp"
#include "castor/tape/tapeserver/RAO/RAOManager.hpp"

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
     * Pop jobs from the queue to reach a threshold of files held by the injector
     * This threshold is either m_maxBatchBytes/m_maxBatchFiles or the values supported by the
     * RAO implementation
     *
     * @param noFilesToRecall will be true if no files were popped from the queue during the call
     * @return true if there are jobs left to be done on the injector, false otherwise
     */
  bool synchronousFetch(bool & noFilesToRecall);

  /**
   * Wait for the inner thread to finish
   */
  void waitThreads();

  /**
   * Start the inner thread
   */
  void startThreads();

  /**
   * Reserve disk space in the eos instance buffer for the next job batch to be injected
   */
  bool reserveSpaceForNextJobBatch(const bool useRAOManager = true);

  /**
   * Set the drive interface in use
   * @param di - Drive interface
   */
  void setDriveInterface(castor::tape::tapeserver::drive::DriveInterface *di);

  /**
   * Initialize Recommended Access Order parameters
   */
  void initRAO(const castor::tape::tapeserver::rao::RAOParams & dataConfig, cta::catalogue::Catalogue * catalogue);

  void waitForPromise();

  void setPromise();

  /**
   * This method will tell the TapeReadSingleThread that the
   * first batch of tasks has been injected
   * by the RecallTaskInjector
   */
  void setFirstTasksInjectedPromise();

  /**
   * This method will be called by the TapeReadSingleThread
   * so that TapeReadSingleThread will wait the first batch
   * of tasks to be injected by the RecallTaskInjector
   */
  void waitForFirstTasksInjectedPromise();

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
   * Transfers a batch of tape-read and write-disk tasks for set of files to retrieve
   * The size of the batch is given by m_maxBatchFiles or m_maxBatchBytes
   */
  void injectBulkRecalls();

  /**
   * Get the next job batch to be injected (but keep it in the injector)
   * @param useRAO wether RAO is supported by the drive or not
   * @return the job batch
   */
  std::list<cta::RetrieveJob*> previewGetNextJobBatch(bool useRAO);

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

  class DiskSpaceReservationStatus {
    cta::DiskSpaceReservationRequest reservedSpace;
    cta::DiskSpaceReservationRequest usedSpace;
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

  /// the amount of disk space reserved and used by the session
  cta::DiskSpaceReservationRequest m_reservedFreeSpace;

  /// Drive interface needed for performing Recommended Access Order query
  castor::tape::tapeserver::drive::DriveInterface * m_drive;

  std::vector<std::unique_ptr<cta::RetrieveJob>> m_jobs;

  /**
   * utility member to log some pieces of information
   */
  cta::log::LogContext m_lc;

  cta::threading::Mutex m_producerProtection;
  cta::threading::BlockingQueue<Request> m_queue;


  //maximal number of files provided by the recall threads at once
  const uint64_t m_maxBatchFiles;

  //maximal number of cumulated byte provided to the recall threads at once
  const uint64_t m_maxBatchBytes;

  //number of files currently held by the recall injector
  uint64_t m_files;

  //number of bytes currently held by the recall injector
  uint64_t m_bytes;


  /**
   * The RAO manager to perofrm RAO operations
   */
  castor::tape::tapeserver::rao::RAOManager m_raoManager;

  /** Number of jobs to be fetched before the tape is mounted.
   *  The desired number is m_raoLimits.maxSupported
   */
  unsigned int m_fetched;

  /**
   * The promise for reordering the read tasks according to RAO by the
   * RecallTaskInjector. The tasks to be run are placed in the m_tasks queue
   */
  std::promise<void> m_raoPromise;
  std::future<void> m_raoFuture;

  std::promise<void> m_firstTasksInjectedPromise;
  std::future<void> m_firstTasksInjectedFuture;

  bool m_promiseFirstTaskInjectedSet = false;
  bool m_sessionEndSignaled = false;

};

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor
