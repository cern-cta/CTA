/******************************************************************************
 *                      DynamicThreadPool.hpp
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
 * @(#)$RCSfile: DynamicThreadPool.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2008/11/04 11:02:02 $ $Author: murrayc3 $
 *
 * This header file describes the implementation of a generic thread pool with
 * dynamic thread creation and destruction. A thread pool is a set of threads
 * which can be created in advance or on demand until a maximum number. When a
 * task is added/pushed to the pool, the thread pool will find an idle thread
 * to handle the task. In case all existing threads are busy and the number of
 * tasks in the queue is higher than the adjustable threshold, the pool will
 * try to create a new threads to serve the tasks if the maximum has not 
 * already been reached. If the number of pending tasks is below the threshold
 * then a thread will be destroyed every 10 seconds until either the threshold
 * or initial number of threads is reached.
 *
 * Note: This class cannot be used directly and must be used from a derived
 * class which implements the execute method.
 *
 * The base implementation is adapted from the following two projects:
 * @see apr_thread_pool.h (Apache Portable Runtime Utils)
 * @see threadpool.h      (LEMON)
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef DYNAMIC_THREAD_POOL_HPP
#define DYNAMIC_THREAD_POOL_HPP 1

// Include files
#include "castor/server/Queue.hpp"
#include "castor/server/IThread.hpp"
#include "castor/exception/Exception.hpp"
#include <pthread.h>
#include <errno.h>
#include <queue>

// Defaults
#define DEFAULT_THREAD_STACKSIZE 262144 // Bytes
#define DEFAULT_INITTHREADS      5
#define DEFAULT_MAXTHREADS       20
#define DEFAULT_THRESHOLD        50
#define DEFAULT_MAXTASKS         1000

#define MAX_THREADPOOL_SIZE      512


namespace castor {

  namespace server {

    /**
     * DynamicThreadPool
     */
    class DynamicThreadPool: public virtual castor::server::IThread {

    public:

      /**
       * Default Constructor.
       * @param initThreads The number of threads to initially be created in
       * the pool.
       * @param maxThreads The maximum number of threads that can be created.
       * Note: This value is capped to MAX_THREADPOOL_SIZE number of threads.
       * Any attempt to set more threads then this will result in an EINVAL
       * exception being thrown.
       * @param threshold The percentage of pending tasks in the queue that
       * must be reached before new threads are created. For example, a value
       * of 10 means that when the task queue is 10% full, the thread pool will
       * begin to create new threads up to the maximum in order to help process
       * the backlog.
       * @param maxTasks The maximum number of tasks that are allowed to be
       * queued pending execution by the threads in the pool.
       * @exception Exception in case of error
       */
      DynamicThreadPool(unsigned int initThreads = DEFAULT_INITTHREADS,
			unsigned int maxThreads  = DEFAULT_MAXTHREADS,
			unsigned int threshold   = DEFAULT_THRESHOLD,
			unsigned int maxTasks    = DEFAULT_MAXTASKS)
	throw(castor::exception::Exception);

      /**
       * Default Destructor
       */
      virtual ~DynamicThreadPool() throw();

      /**
       * Add a task to the thread pool
       * @param data A pointer to the data to add to the task queue.
       * @param wait Flag to indicate if the method should return immediately
       * if the the maximum number of tasks has been reached.
       * @exception Exception in case of error, with one of the following error
       * codes:
       *   EPERM:  Thread pool termination in progress. The callee should give 
       *           up and never call again!
       *   EAGAIN: The task queue is full and the callee has requested the call
       *           to be non-blocking.
       */
      void addTask(void *data, bool wait = true)
	throw(castor::exception::Exception);

      /**
       * Not implemented (MUST BE THREAD SAFE)
       * @param arg The task extracted from the task queue
       */
      virtual void execute(void *arg) = 0;       

    private:

      /**
       * The start routine for threads in the pool.
       * @param arg A pointer to the thread pool object. This is needed so that
       * the threads can access the thread pools internal counters.
       */
      static void* _consumer(void *arg);

    private:

      /// Mutex to protect access counters
      pthread_mutex_t m_lock;

      /// Thread attributes
      pthread_attr_t m_attr;

      /// A FIFO bounded queue storing tasks pending execution
      castor::server::Queue m_taskQueue;

      /// Flag to indicate whether the thread pool is terminated
      bool m_terminated;

      /// The maximum number of threads allowed in the pool
      unsigned int m_maxThreads;

      /// The initial (minimum) number of threads
      unsigned int m_initThreads;

      /// The total number of threads in the pool
      unsigned int m_threadCount;  

      /// Percentage occupancy of the queue that must be reached before new
      /// threads are created
      unsigned int m_threshold;

      /// The last time in seconds since EPOCH that a thread was destroyed.
      u_signed64 m_lastPoolShrink;

    };

  } // End of namespace server

} // End of namespace castor

#endif // DYNAMIC_THREAD_POOL_HPP
