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
 * @(#)$RCSfile: DynamicThreadPool.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/10/21 08:36:38 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/server/DynamicThreadPool.hpp"


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::server::DynamicThreadPool::DynamicThreadPool
(unsigned int initThreads, 
 unsigned int maxThreads,
 unsigned int threshold,
 unsigned int maxTasks)
  throw(castor::exception::Exception) :
  m_terminated(false),
  m_threadCount(0),
  m_lastPoolShrink(0) {

  // Variables
  pthread_t t;
  int       rv;

  // Prevent users setting an extremely large thread pool size.
  if (maxThreads > MAX_THREADPOOL_SIZE) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "maxThreads cannot exceed " << MAX_THREADPOOL_SIZE;
    throw e;
  }

  // Threadpool defaults
  m_maxThreads  = maxThreads;
  m_initThreads = initThreads > m_maxThreads ? m_maxThreads : initThreads;
  m_threshold   = 
    ((m_initThreads == m_maxThreads) || (threshold > 100)) ? 0 :
    (maxTasks / 100) * threshold;

  // Initialize the task queue
  m_taskQueue = new castor::server::Queue(maxTasks);

  // Initialize globus mutexes
  rv = pthread_mutex_init(&m_lock, NULL);
  if (rv != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to pthread_mutex_init(m_lock)";
    throw e;
  }

  // Initialize thread attributes
  pthread_attr_init(&m_attr);
  pthread_attr_setdetachstate(&m_attr, 1);
  pthread_attr_setstacksize(&m_attr, DEFAULT_THREAD_STACKSIZE);
  
  // Create the initial pool of threads. The threads themselves just act as
  // consumers of the task queue and wait for tasks to process.
  for (unsigned int i = 0; i < m_initThreads; i++) {
    rv = pthread_create(&t, &m_attr, (void *(*)(void *))&castor::server::DynamicThreadPool::_consumer, this);
    if (rv != 0) {
      break;
    }
    m_threadCount++;
  }

  // Thread creation failed. Note: exceptions in constructors do not call the
  // destructor so we flag the thread pool for termination so that any threads
  // that were created successfully destroy themselves.
  if (rv != 0) {
    delete m_taskQueue;
    m_terminated = true;

    // Throw exception
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to create " << m_initThreads << " initial "
		   << "threads in the pool";
    throw e;
  }
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::server::DynamicThreadPool::~DynamicThreadPool() throw() {

  // Update the termination flag to true and notify the task queue of the
  // termination.
  m_terminated = true;
  m_taskQueue->terminate();

  // Wait for all threads to shutdown before destroying the queue
  while (m_threadCount > 0) {
    usleep(20 * 1000); // 20 millisecond spin lock
  }
  delete m_taskQueue;

  // Destroy global mutexes
  pthread_mutex_destroy(&m_lock);

  // Destroy thread attributes
  pthread_attr_destroy(&m_attr);
}


//-----------------------------------------------------------------------------
// Consumer
//-----------------------------------------------------------------------------
void* castor::server::DynamicThreadPool::_consumer(void *arg) {

  // Variables
  DynamicThreadPool *pool = (DynamicThreadPool *)arg;

  // Task processing loop.
  while (!pool->m_terminated) {
 
    // Wait for a task to be available from the queue.
    void *args = 0;
    try {
      args = pool->m_taskQueue->pop
	((pool->m_threadCount == pool->m_initThreads));
    } catch (castor::exception::Exception e) {
      if (e.code() == EPERM) {
	break;          // Queue terminated, destroy thread
      } else if (e.code() == EINTR) {
	continue;       // Interrupted
      } else if (e.code() ==  EAGAIN) {
	// As we are in non-blocking mode we sleep very briefly to prevent
	// thrashing of the thread pool
	usleep(5000);
      }
    }
  
    // Invoke the user defined execute method
    try {
      if (args != 0)
	pool->execute(args);
    } catch (...) {
      // Do nothing. It is the responsibility of the user implementing the
      // run method to log errors.
    }

    // Destroy any threads over the initial thread limit if the number of tasks
    // in the task queue has dropped to acceptable limits.
    pthread_mutex_lock(&pool->m_lock);
    if ((pool->m_taskQueue->size() < pool->m_threshold) &&
	(pool->m_threadCount > pool->m_initThreads) &&
	(time(NULL) - pool->m_lastPoolShrink > 10)) {
      pool->m_threadCount--;
      pool->m_lastPoolShrink = time(NULL);

      pthread_mutex_unlock(&pool->m_lock);
      pthread_exit(0);
      return NULL;  // Should not be here
    }
    pthread_mutex_unlock(&pool->m_lock);
  }

  // Register thread destruction
  pthread_mutex_lock(&pool->m_lock);
  pool->m_threadCount--;
  pthread_mutex_unlock(&pool->m_lock);

  pthread_exit(0);
  return NULL;  // Should not be here
}


//-----------------------------------------------------------------------------
// Add Task
//-----------------------------------------------------------------------------
void castor::server::DynamicThreadPool::addTask(void *data, bool wait)
  throw(castor::exception::Exception) {

  // Variables
  pthread_t t;
  int       rv;

  // Push the task to the queue. We ignore interrupts here!
  while (1) {
    try {
      m_taskQueue->push(data, wait);
      break;
    } catch (castor::exception::Exception e) {
      if (e.code() == EINTR) {
	continue; // Interrupted
      }
      throw e;
    }
  }

  // Check to see if additional threads need to be started
  pthread_mutex_lock(&m_lock);
  if ((m_threadCount == 0) ||
      ((m_threadCount < m_maxThreads) &&
       (m_taskQueue->size() > m_threshold))) {
    rv = pthread_create(&t, &m_attr, (void *(*)(void *))&castor::server::DynamicThreadPool::_consumer, this);
    if (rv == 0) {
      m_threadCount++;
    }
  }
  pthread_mutex_unlock(&m_lock);
}
