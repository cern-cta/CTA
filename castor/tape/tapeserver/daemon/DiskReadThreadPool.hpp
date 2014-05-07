/******************************************************************************
 *                      DiskReadThreadPool.hpp
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

#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
#include "castor/log/LogContext.hpp"
#include <vector>
#include <stdint.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  class MigrationTaskInjector;
class DiskReadThreadPool {
public:
  /**
   * Constructor. The constructor creates the threads for the pool, but does not
   * start them.
   * @param nbThread Number of thread for reading files 
   * @param maxFilesReq maximal number of files we might require 
   * within a single request to the task injectore
   * @param maxBytesReq maximal number of bytes we might require
   *  within a single request a single request to the task injectore
   * @param lc log context fpr logging purpose
   */
  DiskReadThreadPool(int nbThread, unsigned int maxFilesReq,unsigned int maxBytesReq, 
          castor::log::LogContext lc);
  
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
   * Get the next task to execute and if there is not enough tasks in queue,
   * it will ask the TaskInjector to get more jobs.
   * @return the next task to execute
   */
  DiskReadTask* popAndRequestMore(castor::log::LogContext & lc);
  
  /**
   * Subclass of the thread pool's worker thread.
   */
  class DiskReadWorkerThread: private castor::tape::threading::Thread {
  public:
    DiskReadWorkerThread(DiskReadThreadPool & parent):
    m_parent(parent),m_threadID(parent.m_nbActiveThread++),m_lc(parent.m_lc) {
       log::LogContext::ScopedParam param(m_lc, log::Param("threadID", m_threadID));
       m_lc.log(LOG_INFO,"DiskWrite Thread created");
    }
    void start() { castor::tape::threading::Thread::start(); }
    void wait() { castor::tape::threading::Thread::wait(); }
  private:
    /** Pointer to the thread pool, allowing calls to popAndRequestMore,
     * and calling finish() on the task injector when the last thread
     * is finishing (thanks to the actomic counter m_parent.m_nbActiveThread) */
    DiskReadThreadPool & m_parent;
    /** The sequential ID of the thread, used in logs */
    const int m_threadID;
    /** The local copy of the log context, allowing race-free logging with context
     between threads. */
    castor::log::LogContext m_lc;
    /** The execution thread: pops and executes tasks (potentially asking for
     more) and calls task injector's finish() on exit of the last thread. */
    virtual void run();
  };
  /** Container for the threads */
  std::vector<DiskReadWorkerThread *> m_threads;
  /** The queue of pointer to tasks to be executed. We own the tasks (they are 
   * deleted by the threads after execution) */
  castor::tape::threading::BlockingQueue<DiskReadTask *> m_tasks;
  /** The log context. This is copied on construction to prevent interferences
   * between threads.
   */
  castor::log::LogContext m_lc;
  /** Pointer to the task injector allowing request for more work, and 
   * termination signalling */
  MigrationTaskInjector* m_injector;
  /** The maximum number of files we ask per request. This value is also used as
   * a threashold (half of it, indeed) to trigger the request for more work. 
   * Another request for more work is also triggered when the task FIFO gets empty.*/
  const uint64_t m_maxFilesReq;
  /** Same as  m_maxFilesReq for size per request. */
  const uint64_t m_maxBytesReq;
  /** An atomic (i.e. thread safe) counter of the current number of thread (they
   are counted up at creation time and down at completion time) */
  tape::threading::AtomicCounter<int> m_nbActiveThread;
};

}}}}
