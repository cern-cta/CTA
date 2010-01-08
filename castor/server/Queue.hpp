/******************************************************************************
 *                      Queue.hpp
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
 * @(#)$RCSfile: Queue.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/10/21 08:36:38 $ $Author: waldron $
 *
 * Simple implementation of a thread safe FIFO bounded queue
 *
 * The base implementation is adapted from the following project:
 * @see apr_queue.h (Apache Portable Runtime Utils)
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef QUEUE_HPP
#define QUEUE_HPP 1

// Include files
#include <sys/time.h>
#include <castor/exception/Exception.hpp>
#include <pthread.h>
#include <errno.h>
#include <queue>

// Defaults
#define DEFAULT_QUEUE_BOUNDS 10000 // Default number of elements in the queue


namespace castor {

  namespace server {
    
    /**
     * A queue entry. It contains a generic parameter
     * and a timestamp for the queuing time.
     */
    struct QueueElement {
      void* param;
      timeval qTime;
    };

    /**
     * Thread Safe FIFO bounded queue
     */
    class Queue {

    public:

      /**
       * Default Constructor
       * @param size The maximum number of elements allowed in the queue.
       * @exception Exception in case of error.
       */
      Queue(unsigned int size = DEFAULT_QUEUE_BOUNDS)
        throw(castor::exception::Exception);

      /**
       * Default Destructor
       */
      ~Queue();

      /**
       * Add an element to the end of the queue.
       * @param data A pointer to the data to add to the queue
       * @param wait Flag to indicate if the method should return immediately
       * if the queue is full or wait until space becomes available.
       * @exception Exception in case of error, with one of the following error
       * codes:
       *   EPERM:  Queue termination in progress. The callee should give up and
       *           never call again!
       *   EAGAIN: The queue is full and the callee has requested the call to 
       *           be non-blocking.
       *   EINTR:  The blocking was interrupted (try again)
       * @warning On queue termination via the destructor the contains of queue
       * will be cleared. As a consequence of this its imperative that the data
       * pushed onto the queue has a destructor which free()'s all the memory
       * allocated to the object. If not, memory leaks will be observed.
       */
      void push(void *data, bool wait = true)
        throw(castor::exception::Exception);
      
      /**
       * Retrieve an element from the front of the queue
       * @param wait Flag to indicate if the method should return immediately
       * if the queue is empty or wait until elements are added/pushed.
       * @return The data
       * @exception Exception in case of error, with one of the following error
       * codes:
       *   EPERM:  Queue termination in progress. The callee should give up and
       *           never call again!
       *   EAGAIN: The queue is empty and the callee has requested the call to 
       *           be non-blocking.
       *   EINTR:  The blocking was interrupted (try again)
       */
      void pop(bool wait, QueueElement& qe)
        throw(castor::exception::Exception);

      /**
       * Returns the number of elements in the queue
       * @return The number of elements in the queue, 0 if the queue is in the
       * process of being terminated.
       */
      unsigned int size();
      
      /**
       * Returns the maximum size of the queue
       * @return The maximum size of the queue
       */
       unsigned int maxSize() {
         return m_bounds;
       }

      /**
       * Terminate the queue, sending an interrupt to all threads waiting on
       * the conditional mutexes.
       */
      void terminate();

    private:

      /// Standard STL queue.
      std::queue<QueueElement> m_queue;

      /// Maximum number of elements in the queue
      unsigned int m_bounds;

      /// Flag to indicate whether the queue is terminated. I.e no more reading
      /// or writing is allowed
      bool m_terminated;

      /// Global queue level lock
      pthread_mutex_t m_lock;

      /// Conditional mutex used to wake up threads who are blocked, waiting
      /// to write into the queue when the queue is full and an element has 
      /// just been removed. (i.e space is now available in the queue)
      pthread_cond_t m_writers;

      /// Conditional mutex used to wake up threads who are blocked, waiting
      /// to read from the queue when the queue is empty and a new element has
      /// been added. (i.e. there is something to be read)
      pthread_cond_t m_readers;

      /// The number of threads waiting on the m_writers conditional mutex
      unsigned int m_nbWriters;

      /// The number of threads waiting on the m_readers contitional mutex
      unsigned int m_nbReaders;

    };

  } // End of namespace server

} // End of namespace castor

#endif // QUEUE_HPP
