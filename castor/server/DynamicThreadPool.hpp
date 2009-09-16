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
 * @(#)$RCSfile: DynamicThreadPool.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2009/08/10 15:27:12 $ $Author: itglp $
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
 * Note: this thread pool is not suitable for database interaction because 
 * dropping and recreating db connections creates too much overhead on Oracle.
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
#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/IThread.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/Queue.hpp"
#include <pthread.h>
#include <errno.h>
#include <queue>

// Defaults
#define DEFAULT_THREAD_STACKSIZE 262144 // Bytes
#define DEFAULT_INITTHREADS      5
#define DEFAULT_MAXTHREADS       20
#define DEFAULT_THRESHOLD        50
#define DEFAULT_MAXTASKS         50

#define MAX_THREADPOOL_SIZE      512


namespace castor {

  namespace server {

    /**
     * DynamicThreadPool
     */
    class DynamicThreadPool: public BaseThreadPool {

    public:

      /**
       * Empty constructor
       */
      DynamicThreadPool() : 
         BaseThreadPool(), m_producerThread(0),
         m_initThreads(0), m_maxThreads(0), m_threshold(0) {};

      /**
       * Default Constructor.
       * @param poolName As in BaseThreadPool
       * @param consumerThread The consumer thread, passed to BaseThreadPool
       * @param producerThread The producer thread: there will be a single
       * instance running this thread, having as param of its run() method
       * this pool instance.
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
      DynamicThreadPool(const std::string poolName,
        castor::server::IThread* consumerThread,
        castor::server::IThread* producerThread,
        unsigned int initThreads = DEFAULT_INITTHREADS,
        unsigned int maxThreads  = DEFAULT_MAXTHREADS,
        unsigned int threshold   = DEFAULT_THRESHOLD,
        unsigned int maxTasks    = DEFAULT_MAXTASKS)
        throw(castor::exception::Exception);

      /**
       * Default Destructor
       */
      virtual ~DynamicThreadPool() throw();

      /**
       * Pre-daemonization initialization. Empty for this pool.
       */
      virtual void init() throw (castor::exception::Exception) {};

      /**
       * Shutdowns the pool by terminating the task queue.
       * @return true if the pool has stopped.
       */
      virtual bool shutdown(bool wait = true) throw();
      
      /**
       * Sets the number of threads
       */
      virtual void setNbThreads(unsigned int value);

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
       * Starts the threads in suspended mode
       */
      virtual void run() throw (castor::exception::Exception);

    private:

      /**
       * The start routine for the producer thread for the pool
       * @param arg A pointer to the thread pool object. This is needed so that
       * the threads can access the thread pools internal data.
       */
      static void* _producer(void *arg);

      /**
       * The start routine for threads in the pool.
       * @param arg A pointer to the thread pool object. This is needed so that
       * the threads can access the thread pools internal counters.
       */
      static void* _consumer(void *arg);

      /// Mutex to protect access counters
      pthread_mutex_t m_lock;

      /// Thread attributes
      pthread_attr_t m_attr;

      /// A FIFO bounded queue storing tasks pending execution
      castor::server::Queue m_taskQueue;

    protected:

      /// The producer thread
      castor::server::IThread* m_producerThread;

      /// The initial (minimum) number of threads
      unsigned int m_initThreads;

      /// The maximum number of threads allowed in the pool
      unsigned int m_maxThreads;

      /// Percentage occupancy of the queue that must be reached before new
      /// threads are created
      unsigned int m_threshold;

      /// The last time in seconds since EPOCH that a thread was destroyed.
      u_signed64 m_lastPoolShrink;
    };

  } // End of namespace server

} // End of namespace castor

#endif // DYNAMIC_THREAD_POOL_HPP
