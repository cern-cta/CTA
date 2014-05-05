/******************************************************************************
 *                      DiskWriteThreadPool.hpp
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

#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
#include "castor/tape/tapeserver/daemon/TaskInjector.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "DiskWriteTask.hpp"
#include <vector>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
/**
 * Container for the threads that will execute the disk writes tasks in the 
 * migration.
 */
class DiskWriteThreadPool {
public:
  /**
   * Constructor: we create the thread structures here, but they do not get
   * started yet.
   * @param nbThread Fixed number of threads in the pool
   * @param reportPacker Reference to a previously created recall
   * report packer, to which the tasks will report their results.
   * @param lc reference to a log context object that will be copied at
   * construction time (and then copied further for each thread). There will
   * be no side effect on the caller's logs.
   */
  DiskWriteThreadPool(int nbThread, 
          ReportPackerInterface<detail::Recall>& reportPacker,
          castor::log::LogContext lc);
  /**
   * Destructor: we suppose the threads are no running (waitThreads() should
   * be called befor destruction unless the threads were not started.
   */
  ~DiskWriteThreadPool();
  
  /**
   * Starts the thread created at construction time.
   */
  
  void startThreads();
  /**
   * Waits for completion of all the pool's threads.
   */
  void waitThreads();
  
  /**
   * Pushes a pointer to a task. The thread pool owns the task and will
   * de-allocate it.
   * @param t pointer to the task
   */
  void push(DiskWriteTask *t);
  
  /**
   * Signals to the thread pool that there will be no more tasks pushed to it,
   * and that the threads can therefore complete.
   */
  void finish();

private:
  /** Running counter active threads, used to determine which thread is the last. */
  tape::threading::AtomicCounter<int> m_nbActiveThread;
  /** Thread safe counter for failed tasks */
  tape::threading::AtomicCounter<int> m_failedWriteCount;
  
  /**
   * Private class implementing the worker threads.
   */
  class DiskWriteWorkerThread: private castor::tape::threading::Thread {
  public:
    DiskWriteWorkerThread(DiskWriteThreadPool & manager):
    m_threadID(manager.m_nbActiveThread++),m_parentThreadPool(manager),m_lc(m_parentThreadPool.m_lc)
    {
      // This thread Id will remain for the rest of the thread's lifetime (and 
      // also context's lifetime) so ne need for a scope.
      m_lc.pushOrReplace(log::Param("threadID", m_threadID));
      m_lc.log(LOG_INFO,"DiskWrite Thread created");
    }
      
    void start() { castor::tape::threading::Thread::start(); }
    void wait() { castor::tape::threading::Thread::wait(); }
  private:
    const int m_threadID;
    DiskWriteThreadPool & m_parentThreadPool;
    castor::log::LogContext m_lc;
    virtual void run();
  };
  
  /** The actual container for the thread objects */
  std::vector<DiskWriteWorkerThread *> m_threads;
  /** Mutex protecting the pushers of new tasks from having the object deleted
   * under their feet. */
  castor::tape::threading::Mutex m_pusherProtection;
protected:
  /** The (thread safe) queue of tasks */
  castor::tape::threading::BlockingQueue<DiskWriteTask*> m_tasks;
private:
  /** Reference to the report packer where tasks report the result of their 
   * individual files and the end of session (for the last thread) */
  ReportPackerInterface<detail::Recall>& m_reporter;
  
  /** logging context that will be copied by each thread for individual context */
  castor::log::LogContext m_lc;
};

}}}}
