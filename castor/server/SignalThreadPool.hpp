/******************************************************************************
 *                      SignalThreadPool.hpp
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
 * @(#)$RCSfile: SignalThreadPool.hpp,v $ $Revision: 1.16 $ $Release$ $Date: 2008/04/02 17:34:54 $ $Author: itglp $
 *
 * Thread pool supporting wakeup on signals and periodical run after timeout
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_SIGNALTHREADPOOL_HPP
#define CASTOR_SERVER_SIGNALTHREADPOOL_HPP 1

/* ============== */
/* System headers */
/* ============== */
#include <errno.h>                      /* For EINVAL etc... */
#include "osdep.h"
#include <sys/types.h>
#include <iostream>
#include <string>

/* ============= */
/* Local headers */
/* ============= */
#include "serrno.h"                     /* CASTOR error numbers */
#include "Cnetdb.h"                     /* For Cgetservbyname() */
#include "Cthread_api.h"

#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/Mutex.hpp"
#include "castor/server/NotifierThread.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"


namespace castor {

 namespace server {

  // Forward declaration
  class Mutex;

  /**
   * CASTOR thread pool supporting wakeup on signals
   * and periodical run after timeout.
   * Credits to Jean-Damien Durand for the underlying C code.
   */
  class SignalThreadPool : public BaseThreadPool {

  /**
   * NotifierThread and ServiceThread must be friend as 
   * they closely interact with the internal mutex for
   * waking up waiting threads or pausing them
   */
  friend class NotifierThread;
  friend class ServiceThread;

  public:

    /**
     * empty constructor
     */
     SignalThreadPool() :
       BaseThreadPool(), m_poolMutex(0) {};

    /**
     * constructor
     */
    SignalThreadPool(const std::string poolName,
                   castor::server::IThread* thread,
                   const int timeout = castor::server::Mutex::TIMEOUT);

    /**
     * destructor
     */
     virtual ~SignalThreadPool() throw();

    /**
     * Initializes the pool
     */
    virtual void init() throw (castor::exception::Exception);

    /**
     * Creates and runs the pool starting the threads in detached mode.
     * Moreover, it starts the notification thread for this pool.
     */
    virtual void run() throw (castor::exception::Exception);

    /**
     * Shutdowns the pool. Waits for all thread of this pool to end.
     * @return true iff no thread is active (i.e. m_nbActiveThreads == 0).
     */
    virtual bool shutdown() throw ();

    /**
     * Gets this thread pool's mutex.
     * used by threads for thread-safe operations.
     * @return m_poolMutex.
     */
    Mutex* getMutex() {
      return m_poolMutex;
    }

    /**
     * Get this thread pool's active threads count.
     * The function is not taking any lock, hence its clients
     * (e.g. BaseDaemon::waitAllThreads()) shall handle
     * this pool's mutex accordingly.
     * @return int m_nbActiveThreads
     */
    int getActiveThreads()
      throw (castor::exception::Exception) {
      return m_nbActiveThreads;
    }

  private:

    /**
     * Commit a thread in the list of active threads
     * for this pool. Uses the internal mutex to be thread-safe.
     * @throw exception if a mutex call fails
     */
    void commitRun()
      throw (castor::exception::Exception);

    /**
     * Waits for a signal or for a timeout.
     * Uses the internal mutex to be thread-safe.
     * @throw exception if a mutex call fails
     */
    void waitSignalOrTimeout()
      throw (castor::exception::Exception);

    /**
     * Release a thread from the list of active threads
     * for this pool. Uses the internal mutex to be thread-safe.
     */
    void commitRelease();

    /// timeout between two subsequent wake ups of the underlying threads
    int m_timeout;

    /// count of the current number of busy threads in the pool
    int m_nbActiveThreads;
    
    /// if > 0, nb of threads that need to be signaled
    int m_notified;
    
    /// flag used to make sure a thread is started straight away at startup
    bool m_notTheFirstTime;

    /// mutex used by the threads to safely access this class' fields
    Mutex* m_poolMutex;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_SIGNALTHREADPOOL_HPP
