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

#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "disk/RadosStriperPool.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/threading/Thread.hpp"
#include "common/threading/AtomicCounter.hpp"
#include "common/log/LogContext.hpp"
#include "common/Timer.hpp"
#include <vector>
#include <stdint.h>

namespace castor::tape::tapeserver::daemon {

  class MigrationTaskInjector;
class DiskReadThreadPool {
public:
  /**
   * The constructor creates the threads for the pool, but does not start them.
   *
   * @param nbThread    Number of thread for reading files 
   * @param maxFilesReq Maximum number of files we might require within a single request to the task injector
   * @param maxBytesReq Maximum number of bytes we might require within a single request a single request
   *                    to the task injector
   * @param lc          Log context for logging purpose
   */
  DiskReadThreadPool(int nbThread, uint64_t maxFilesReq, uint64_t maxBytesReq, 
          castor::tape::tapeserver::daemon::MigrationWatchDog& migrationWatchDog,
          cta::log::LogContext lc,
          uint16_t xrootTimeout);
  
  /**
   * Destructor.
   * Simply destroys the thread, which should have been joined by the caller
   * before using waitThreads()
   */
  ~DiskReadThreadPool();
  
  /**
   * Starts the threads which were created at construction time.
   */
  void startThreads();
  
  /**
   * Waits for threads completion of all threads. Should be called before
   * destructor
   */
  void waitThreads();
  
  /**
   * Adds a DiskReadTask to the tape pool's queue of tasks.
   * @param task pointer to the new task. The thread pool takes ownership of the
   * task and will delete it after execution. Push() is not protected against races
   * with finish() as the task injection is done from a single thread (the task
   * injector)
   */
  void push(DiskReadTask *task);
  
  /**
   * Injects as many "end" tasks as there are threads in the thread pool.
   * A thread getting such an end task will exit. This method is called by the
   * task injector at the end of the tape session. It is not race protected. 
   * See push()
   */
  void finish();
  
  /**
   * Sets up the pointer to the task injector. This cannot be done at
   * construction time as both task injector and read thread pool refer to 
   * each other. This function should be called before starting the threads.
   * This is used for the feedback loop where the injector is requested to
   * fetch more work by the read thread pool when the task queue of the thread
   * pool starts to run low.
   */
  void setTaskInjector(MigrationTaskInjector* injector){
      m_injector = injector;
  }
private:
  /**
   * When the last thread finish, we log all m_pooldStat members + message
   * at the given level
   * @param level
   * @param message
   */
  void logWithStat(int level, const std::string& message);
  /** 
   * Get the next task to execute and if there is not enough tasks in queue,
   * it will ask the TaskInjector to get more jobs.
   * @return the next task to execute
   */
  DiskReadTask* popAndRequestMore(cta::log::LogContext & lc);
  
  /**
   * When a thread finishm it call this function to Add its stats to one one of the
   * Threadpool
   * @param threadStats
   */
  void addThreadStats(const DiskStats& stats);
  
  /**
   To protect addThreadStats from concurrent calls
   */
  cta::threading::Mutex m_statAddingProtection;
  
  /**
   * Aggregate all threads' stats 
   */
  DiskStats m_pooldStat;
  
  /**
   * Measure the thread pool's lifetime
   */
  cta::utils::Timer m_totalTime;
  
  /**
   * Subclass of the thread pool's worker thread.
   */
  class DiskReadWorkerThread: private cta::threading::Thread {
  public:
    DiskReadWorkerThread(DiskReadThreadPool & parent):
    m_parent(parent),m_threadID(parent.m_nbActiveThread++),m_lc(parent.m_lc),
    m_diskFileFactory(parent.m_xrootTimeout, parent.m_striperPool) {
      cta::log::LogContext::ScopedParam param(m_lc, cta::log::Param("threadID", m_threadID));
      m_lc.log(cta::log::INFO, "DiskReadThread created");
    }
    void start() { cta::threading::Thread::start(); }
    void wait() { cta::threading::Thread::wait(); }
  private:
    void logWithStat(int level, const std::string& message);
    /*
     * For measuring how long  are the the different steps 
     */
    DiskStats m_threadStat;
    
    /** Pointer to the thread pool, allowing calls to popAndRequestMore,
     * and calling finish() on the task injector when the last thread
     * is finishing (thanks to the actomic counter m_parent.m_nbActiveThread) */
    DiskReadThreadPool & m_parent;
    
    /** The sequential ID of the thread, used in logs */
    const int m_threadID;
    
    /** The local copy of the log context, allowing race-free logging with context
     between threads. */
    cta::log::LogContext m_lc;
    
    /** The execution thread: pops and executes tasks (potentially asking for
     more) and calls task injector's finish() on exit of the last thread. */
    virtual void run();

    /**
     * A disk file factory, that will create the proper type of file access class,
     * depending on the received path
     */
    cta::disk::DiskFileFactory m_diskFileFactory;
  };
  
  /** Container for the threads */
  std::vector<DiskReadWorkerThread *> m_threads;
  
  /** The queue of pointer to tasks to be executed. We own the tasks (they are 
   * deleted by the threads after execution) */
  cta::threading::BlockingQueue<DiskReadTask *> m_tasks;
  
  /**
   * Parameter: xroot timeout
   */
  uint16_t m_xrootTimeout;

  /**
   * A pool of rados striper connections, to be shared by all threads
   */
  cta::disk::RadosStriperPool m_striperPool;

  /**
   * Reference to the watchdog, for error reporting.
   */
  castor::tape::tapeserver::daemon::MigrationWatchDog & m_watchdog;
  
  /** The log context. This is copied on construction to prevent interferences
   * between threads.
   */
  cta::log::LogContext m_lc;
  
  /** Pointer to the task injector allowing request for more work, and 
   * termination signaling */
  MigrationTaskInjector* m_injector;
  
  /** The maximum number of files we ask per request. This value is also used as
   * a threshold (half of it, indeed) to trigger the request for more work. 
   * Another request for more work is also triggered when the task FIFO gets empty.*/
  const uint64_t m_maxFilesReq;
  
  /** Same as m_maxFilesReq for size per request. */
  const uint64_t m_maxBytesReq;
  
  /** An atomic (i.e. thread safe) counter of the current number of thread (they
   are counted up at creation time and down at completion time) */
  cta::threading::AtomicCounter<int> m_nbActiveThread;
};

} // namespace castor::tape::tapeserver::daemon
