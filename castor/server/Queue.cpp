/******************************************************************************
 *                      Queue.cpp
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
 * @(#)$RCSfile: Queue.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2009/08/04 09:19:57 $ $Author: murrayc3 $
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/server/Queue.hpp"


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::server::Queue::Queue(unsigned int size) 
  throw(castor::exception::Exception) :
  m_bounds(size),
  m_terminated(false),
  m_nbWriters(0),
  m_nbReaders(0) {
  
  // Variables
  int rv;

  // Initialize global mutexes
  rv = pthread_mutex_init(&m_lock, NULL);
  if (rv != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to pthread_mutex_init(m_lock)";
    throw e;
  }

  // Initialize conditional mutexes
  rv = pthread_cond_init(&m_readers, NULL);
  if (rv != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to pthread_cond_init(m_readers)";
    throw e;
  }

  rv = pthread_cond_init(&m_writers, NULL);
  if (rv != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to pthread_cond_init(m_writers)";
    throw e;
  }
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::server::Queue::~Queue() {

  // Wait for all waiting threads to wake up
  
  while ((m_nbWriters != 0) && 
	 (m_nbReaders != 0)) {
    usleep(20 * 1000); // 20 millisecond spin lock
  }

  // Destroy global mutexes
  pthread_mutex_destroy(&m_lock);

  // Destroy conditional mutexes
  pthread_cond_destroy(&m_readers);
  pthread_cond_destroy(&m_writers);
}


//-----------------------------------------------------------------------------
// Push
//-----------------------------------------------------------------------------
void castor::server::Queue::push(void *data, bool wait)
  throw(castor::exception::Exception) {

  // Check if the queue has been terminated
  if (m_terminated) {
    castor::exception::Exception e(EPERM);     // Operation not permitted
    throw e;
  }

  // Acquire global queue level lock
  pthread_mutex_lock(&m_lock);

  // If there is no space available in the queue and depending on the wait flag
  // we will either block waiting for space to become available or return
  // control to the callee
  if (m_queue.size() == m_bounds) {
    if (!wait) {
      pthread_mutex_unlock(&m_lock);
      castor::exception::Exception e(EAGAIN);  // Try again
      throw e;
    }
    if (!m_terminated) {
      m_nbWriters++;
      pthread_cond_wait(&m_writers, &m_lock);
      m_nbWriters--;
    }
    // If we wake up and the queue is empty we were interrupted
    if (m_queue.size() == m_bounds) {
      pthread_mutex_unlock(&m_lock);
      if (m_terminated) {
        castor::exception::Exception e(EPERM); // Operation not permitted
        throw e;
      } else {
        castor::exception::Exception e(EINTR); // Interrupted
        throw e;
      }
    }
  }

  // Push data to the STL queue container
  QueueElement qe;
  gettimeofday(&qe.qTime, NULL);
  qe.param = data;
  m_queue.push(qe);

  // If we have readers waiting on the queue then we wake one of them up
  if (m_nbReaders) {
    pthread_cond_signal(&m_readers);
  }
  pthread_mutex_unlock(&m_lock);
}


//-----------------------------------------------------------------------------
// Pop
//-----------------------------------------------------------------------------
void castor::server::Queue::pop(bool wait, QueueElement& qe)
  throw(castor::exception::Exception) {
  
  // Check if the queue has been terminated
  if (m_terminated) {
    castor::exception::Exception e(EPERM);     // Operation not permitted
    throw e;
  }

  // Acquire global queue level lock
  pthread_mutex_lock(&m_lock);

  // If the queue is empty and depending on the wait flag we will either block
  // waiting for a new element to be added or return control to the callee
  if (m_queue.empty()) {
    if (!wait) {
      pthread_mutex_unlock(&m_lock);
      castor::exception::Exception e(EAGAIN);  // Try again
      throw e;
    }
    if (!m_terminated) {
      m_nbReaders++;
      pthread_cond_wait(&m_readers, &m_lock);
      m_nbReaders--;
    }
    // If we wake up and the queue is still empty we were interrupted
    if (m_queue.empty()) {
      pthread_mutex_unlock(&m_lock);
      if (m_terminated) {
        castor::exception::Exception e(EPERM); // Operation not permitted
        throw e;
      } else {
        castor::exception::Exception e(EINTR); // Interrupted
	      throw e;
      }
    }
  }
  
  // Extract the first element from the STL queue container
  qe = m_queue.front();
  m_queue.pop();

  // If we have writers waiting on the queue then we wake one of them up
  if (m_nbWriters) {
    pthread_cond_signal(&m_writers);
  }
  pthread_mutex_unlock(&m_lock);
}


//-----------------------------------------------------------------------------
// Size
//-----------------------------------------------------------------------------
unsigned int castor::server::Queue::size() {

  // Check if the queue has been terminated. We don't throw an error here!
  // Instead we just return that the queue is empty.
  if (m_terminated) {
    return 0;
  }

  // Return the number of elements in the queue
  return m_queue.size();
}


//-----------------------------------------------------------------------------
// Terminate
//-----------------------------------------------------------------------------
void castor::server::Queue::terminate() {

  if (m_terminated) {
    return;  // Already terminated
  }

  // Update the termination flag to true and wake up all threads waiting to
  // read or write to the queue. The interrupted threads will see the queue
  // termination and throw an exception.
  pthread_mutex_lock(&m_lock);
  m_terminated = true;

  pthread_cond_broadcast(&m_writers);
  pthread_cond_broadcast(&m_readers);

  pthread_mutex_unlock(&m_lock);
}
