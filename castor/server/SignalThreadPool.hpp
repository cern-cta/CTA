/******************************************************************************
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
 * Thread pool supporting wakeup on signals and periodical run after timeout
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include <errno.h>
#include <sys/types.h>
#include <iostream>
#include <string>

#include "osdep.h"
#include "serrno.h"
#include "Cthread_api.h"

#include "castor/BaseObject.hpp"
#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/AllInOneLockingUtility.hpp"
#include "castor/server/NotifierThread.hpp"
#include "castor/exception/Exception.hpp"


namespace castor {

 namespace server {

  // Forward declaration
  class AllInOneLockingUtility;

  /**
   * CASTOR thread pool supporting wakeup on signals
   * and periodical run after timeout.
   * Credits to Jean-Damien Durand for the underlying C code.
   */
  class SignalThreadPool : public BaseThreadPool {

  /**
   * NotifierThread must be friend as it closely
   * interacts with the internal mutex for
   * waking up waiting threads or pausing them
   */
  friend class NotifierThread;

  public:

    /**
     * Constructor
     * @param poolName, thread as in BaseThreadPool
     * @param timeout the interval in seconds on which
     *        threads will wake up independently from
     *        notifications
     * @param nbThreads total number of threads in the pool
     * @param startingThreads number of threads to be
     *        run immediately at startup, defaults to 1
     */
    SignalThreadPool(const std::string poolName,
                     castor::server::IThread* thread,
                     const int timeout = castor::server::AllInOneLockingUtility::TIMEOUT,
                     const unsigned int nbThreads = DEFAULT_THREAD_NUMBER,
                     const unsigned int startingThreads = 1)
      ;

    /**
     * Destructor
     */
    virtual ~SignalThreadPool() throw();

    /**
     * Creates and runs the pool starting the threads in detached mode.
     */
    virtual void run() ;

    /**
     * Shutdowns the pool.
     * @param wait flag to indicate to wait for all thread of this
     * pool to terminate.
     * @return true iff no thread is active (i.e. m_nbActiveThreads == 0).
     */
    virtual bool shutdown(bool wait = true) throw();

  private:

    /**
     * Waits for a signal or for a timeout.
     * Uses the internal mutex to be thread-safe.
     * @throw Exception if a mutex call fails
     */
    void waitSignalOrTimeout()
      ;

    /// mutex used by the threads to safely access this class' fields
    AllInOneLockingUtility m_poolMutex;
    
    /// if > 0, nb of threads that need to be signaled
    int m_notified;

    /// thread entrypoint
    static void* _runner(void* param);

  };

 } // end of namespace server

} // end of namespace castor


