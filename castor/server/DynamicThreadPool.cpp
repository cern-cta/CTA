/******************************************************************************
 *                      DynamicThreadPool.cpp
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
 * @(#)$RCSfile: DynamicThreadPool.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2009/08/11 09:49:08 $ $Author: itglp $
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include <unistd.h>
#include <sys/time.h>
#include "castor/Services.hpp"
#include "castor/metrics/MetricsCollector.hpp"
#include "castor/metrics/InternalCounter.hpp"
#include "castor/server/DynamicThreadPool.hpp"

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::server::DynamicThreadPool::DynamicThreadPool
(const std::string poolName,
 castor::server::IThread* consumerThread,
 castor::server::IThread* producerThread,
 unsigned int initThreads, 
 unsigned int maxThreads,
 unsigned int threshold,
 unsigned int maxTasks)
  throw(castor::exception::Exception) :
  BaseThreadPool(poolName, consumerThread, 0),
  m_taskQueue(maxTasks),
  m_producerThread(producerThread),
  m_lastPoolChange(0) {

  int rv;

  // Prevent users setting an extremely large thread pool size.
  if (maxThreads > MAX_THREADPOOL_SIZE) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "maxThreads cannot exceed " << MAX_THREADPOOL_SIZE;
    throw e;
  }
  
  // Threadpool defaults
  m_maxThreads  = maxThreads;
  m_initThreads = initThreads > maxThreads ? maxThreads : initThreads;
  m_threshold   = 
    ((m_initThreads == m_maxThreads) || (threshold > 100)) ? 0 :
    (int)(((float)maxTasks / 100.0) * threshold);

  // Initialize global mutexes
  rv = pthread_mutex_init(&m_lock, NULL);
  if (rv != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to pthread_mutex_init(m_lock)";
    throw e;
  }

  // Initialize thread attributes
  rv = pthread_attr_init(&m_attr);
  if (rv != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to pthread_attr_init(m_attr)";
    throw e;
  }
  pthread_attr_setdetachstate(&m_attr, PTHREAD_CREATE_DETACHED);
  pthread_attr_setstacksize(&m_attr, DEFAULT_THREAD_STACKSIZE);
}

//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::server::DynamicThreadPool::~DynamicThreadPool() throw() {
  // Destroy the producer thread
  if(m_producerThread) {
    delete m_producerThread;
  }

  // Destroy global mutexes
  pthread_mutex_destroy(&m_lock);              
  
  // Destroy thread attributes
  pthread_attr_destroy(&m_attr);
}

//-----------------------------------------------------------------------------
// setNbThreads
//-----------------------------------------------------------------------------
void castor::server::DynamicThreadPool::setNbThreads(unsigned int value) {
  if (m_initThreads == m_maxThreads) {
    m_initThreads = m_maxThreads = value;
  } else {
    // Preserve the current offset between init and max if possible
    m_maxThreads += (int)(value - m_initThreads);
    m_initThreads = value;
    if (m_maxThreads < value) {
      m_maxThreads = value;
      m_threshold = 0;
    }
  }
}

//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::server::DynamicThreadPool::init()
  throw (castor::exception::Exception) {
  
  castor::server::BaseThreadPool::init();
  
  // Enable internal monitoring if the metrics collector has already
  // been instantiated by the user application
  castor::metrics::MetricsCollector* mc = castor::metrics::MetricsCollector::getInstance(0);
  if(mc) {
    mc->getHistogram("BacklogFactor")->addCounter(
      new castor::metrics::InternalCounter(*this, "%",
        &castor::server::BaseThreadPool::getBacklogFactor));
    mc->getHistogram("AvgQueuingTime")->addCounter(
      new castor::metrics::InternalCounter(*this, "ms",
        &castor::server::BaseThreadPool::getAvgQueuingTime));
  }
}

//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::server::DynamicThreadPool::run()
  throw (castor::exception::Exception) {  
  pthread_t t;
  int       rv;
  
  if (m_initThreads > 0) {
    // Create the initial pool of threads. The threads themselves just act as
    // consumers of the task queue and wait for tasks to process.
    for (unsigned int i = 0; i < m_initThreads; i++) {
      rv = pthread_create(&t, &m_attr, (void *(*)(void *))&DynamicThreadPool::_consumer, this);
      if (rv != 0) {
        break;
      }
      m_nbThreads++;
    }
    
    // Thread creation failed. We flag the thread pool for termination
    // so that any threads that were created successfully destroy themselves.
    if (rv != 0) {
      m_stopped = true;
      // Throw exception
      castor::exception::Exception e(errno);
      e.getMessage() << "Failed to create " << m_initThreads << " initial "
                     << "threads in the pool";
      throw e;
    }
  }
    
  // Initialize producer thread if requested
  if (0 != m_producerThread) {
    rv = pthread_create(&t, &m_attr, (void *(*)(void *))&DynamicThreadPool::_producer, this);
    if (rv != 0) {
      // Same as above
      m_stopped = true;
      castor::exception::Exception e(errno);
      e.getMessage() << "Failed to create the producer thread";
      throw e;
    }
  }    

  // Threads have been created
  castor::dlf::Param params[] =
    {castor::dlf::Param("ThreadPool", m_poolName),
     castor::dlf::Param("Type", "DynamicThreadPool"),
     castor::dlf::Param("InitThreads", m_initThreads),
     castor::dlf::Param("MaxThreads", m_maxThreads)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                          DLF_BASE_FRAMEWORK + 3, 4, params);
}

//-----------------------------------------------------------------------------
// Shutdown
//-----------------------------------------------------------------------------
bool castor::server::DynamicThreadPool::shutdown(bool wait) throw() {
  // Update the termination flag to true
  m_stopped = true;
  
  // Notify the producer and the consumer threads (via the task queue) to stop
  if(m_producerThread) {
    m_producerThread->stop();
  }
  m_taskQueue.terminate();

  if (wait) {
    // Spin lock to make sure we're over
    while (m_nbThreads > 0) {
      usleep(100000);
    }
  }
  
  // Inform whether we're done
  return (m_nbThreads == 0);
}

//------------------------------------------------------------------------------
// resetMetrics
//------------------------------------------------------------------------------
void castor::server::DynamicThreadPool::resetMetrics()
{
  // see comments in the ancestor
  castor::server::BaseThreadPool::resetMetrics();
  m_queueTime = 0;
}

//-----------------------------------------------------------------------------
// Producer
//-----------------------------------------------------------------------------
void* castor::server::DynamicThreadPool::_producer(void *arg) {
  
  DynamicThreadPool *pool = (DynamicThreadPool *)arg;

  try {
    // Initialize the producer thread
    pool->m_producerThread->init();

    // Run it: this is supposed to run forever
    pool->m_producerThread->run(pool);
  }
  catch(castor::exception::Exception& any) {
    // "Uncaught exception in a thread from pool"
    castor::dlf::Param params[] =
      {castor::dlf::Param("ThreadPool", pool->m_poolName),
       castor::dlf::Param("Error", sstrerror(any.code())),
       castor::dlf::Param("Message", any.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                            DLF_BASE_FRAMEWORK + 10, 3, params);
  } catch(...) {
    // "Uncaught GENERAL exception in a thread from pool"
    castor::dlf::Param params[] =
      {castor::dlf::Param("ThreadPool", pool->m_poolName)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                            DLF_BASE_FRAMEWORK + 11, 1, params);
  }
  try {
    // cleanup thread specific globals
    delete (services());
  } catch (...) {
    // ignore errors
  }
  pthread_exit(0);
  return 0;
}

//-----------------------------------------------------------------------------
// Consumer
//-----------------------------------------------------------------------------
void* castor::server::DynamicThreadPool::_consumer(void *arg) {
  // Variables
  DynamicThreadPool *pool = (DynamicThreadPool *)arg;
  timeval tv1, tv2;
  double activeTime = 0, idleTime = 0, queueTime = 0;
  
  // Thread initialization
  try {
    pool->m_thread->init();
  } catch (castor::exception::Exception& any) {
    // "Thread run error"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", sstrerror(any.code())),
       castor::dlf::Param("Message", any.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                            DLF_BASE_FRAMEWORK + 5, 2, params);
  } 

  gettimeofday(&tv2, NULL);
  
  // Task processing loop.
  while (!pool->m_stopped) {

    // Wait for a task to be available from the queue.
    QueueElement qe;
    qe.param = 0;
    try {
      pool->m_taskQueue.pop(
        (pool->m_nbThreads == pool->m_initThreads), qe);
    } catch (castor::exception::Exception& e) {
      if (e.code() == EPERM) {
        break;          // Queue terminated, destroy thread
      } else if (e.code() == EINTR) {
        continue;       // Interrupted
      } else if (e.code() == EAGAIN) {
        // As we are in non-blocking mode we sleep very briefly to prevent
        // thrashing of the thread pool
        usleep(5000);
      }
    }
    
    // Invoke the user code
    try {
      if (qe.param != 0) {
        // First update counters
        pthread_mutex_lock(&pool->m_lock);
        pool->m_nbActiveThreads++;
        pthread_mutex_unlock(&pool->m_lock);
        gettimeofday(&tv1, NULL);
        idleTime = tv1.tv_sec - tv2.tv_sec + (tv1.tv_usec - tv2.tv_usec)/1000000.0;
        queueTime = tv1.tv_sec - qe.qTime.tv_sec + (tv1.tv_usec - qe.qTime.tv_usec)/1000000.0;

        // User processing
        pool->m_thread->run(qe.param);
        
        // Re-update timing and counters
        gettimeofday(&tv2, NULL);
        activeTime = tv2.tv_sec - tv1.tv_sec + (tv2.tv_usec - tv1.tv_usec)/1000000.0;

        pthread_mutex_lock(&pool->m_lock);
        pool->m_nbActiveThreads--;
        pool->m_runsCount++;
        pthread_mutex_unlock(&pool->m_lock);

        // "Task processed"         
        castor::dlf::Param params[] =
          {castor::dlf::Param("ThreadPool", pool->m_poolName),
           castor::dlf::Param("ProcessingTime", activeTime),
           castor::dlf::Param("QueuingTime", queueTime)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                DLF_BASE_FRAMEWORK + 22, 3, params);
      }
    } catch(castor::exception::Exception& any) {
      // "Uncaught exception in a thread from pool"
      castor::dlf::Param params[] =
        {castor::dlf::Param("ThreadPool", pool->m_poolName),
         castor::dlf::Param("Error", sstrerror(any.code())),
         castor::dlf::Param("Message", any.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                              DLF_BASE_FRAMEWORK + 10, 3, params);
    } catch(...) {
      // "Uncaught GENERAL exception in a thread from pool"
      castor::dlf::Param params[] =
        {castor::dlf::Param("ThreadPool", pool->m_poolName)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                              DLF_BASE_FRAMEWORK + 11, 1, params);
    }
    
    // Destroy any threads over the initial thread limit if the number of tasks
    // in the task queue has dropped to acceptable limits.
    pthread_mutex_lock(&pool->m_lock);
    u_signed64 diff = time(NULL) - pool->m_lastPoolChange;
    // update shared timers
    pool->m_activeTime += activeTime;
    pool->m_idleTime += idleTime;
    pool->m_queueTime += queueTime;
    if ((pool->m_taskQueue.size() < pool->m_threshold) &&
        (pool->m_nbThreads > pool->m_initThreads) &&
        (diff > 30)) {
      pool->m_lastPoolChange = time(NULL);
      pthread_mutex_unlock(&pool->m_lock);
      // Exit the loop and terminate
      break;
    }
    pthread_mutex_unlock(&pool->m_lock);
  }
  
  // "Terminating a thread in pool"
  castor::dlf::Param params[] =
    {castor::dlf::Param("ThreadPool", pool->m_poolName),
     castor::dlf::Param("PendingTasks", pool->m_taskQueue.size())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                          DLF_BASE_FRAMEWORK + 21, 2, params);
  
  // Register thread destruction
  pthread_mutex_lock(&pool->m_lock);
  pool->m_nbThreads--;
  pthread_mutex_unlock(&pool->m_lock);
  
  // Notify we're exiting
  try {
    // thread specific cleanup
    pool->m_thread->stop();
  } catch (castor::exception::Exception& any) {
    // "Thread run error"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", sstrerror(any.code())),
       castor::dlf::Param("Message", any.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                            DLF_BASE_FRAMEWORK + 5, 2, params);
  } 
  try {
    // cleanup thread specific globals
    delete (services());
  } catch (...) {
    // ignore errors
  }
  pthread_exit(0);
  return 0;  // Should not be here
}


//-----------------------------------------------------------------------------
// addTask
//-----------------------------------------------------------------------------
void castor::server::DynamicThreadPool::addTask(void *data, bool wait)
  throw(castor::exception::Exception) {
  
  // Variables
  pthread_t t;
  int       rv;
  
  // Push the task to the queue. We ignore interrupts here!
  while (1) {
    try {
      m_taskQueue.push(data, wait);
      break;
    } catch (castor::exception::Exception& e) {
      if (e.code() == EINTR) {
        continue; // Interrupted
      }
      throw e;
    }
  }
  
  // Check to see if additional threads need to be started
  pthread_mutex_lock(&m_lock);
  if ((m_nbThreads == 0) ||
      ((m_nbThreads < m_maxThreads) &&
       (m_taskQueue.size() > m_threshold))) {
    rv = pthread_create(&t, &m_attr, (void *(*)(void *))&castor::server::DynamicThreadPool::_consumer, this);
    if (rv == 0) {
      m_nbThreads++;
      m_lastPoolChange = time(NULL);
      // "Spawning a new thread in pool"
      castor::dlf::Param params[] =
        {castor::dlf::Param("ThreadPool", m_poolName),
         castor::dlf::Param("PendingTasks", m_taskQueue.size())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                              DLF_BASE_FRAMEWORK + 20, 2, params);
    }
  }
  pthread_mutex_unlock(&m_lock);
}
