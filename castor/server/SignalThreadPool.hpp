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
 * @(#)$RCSfile: SignalThreadPool.hpp,v $ $Revision: 1.10 $ $Release$ $Date: 2006/08/14 19:12:41 $ $Author: itglp $
 *
 *
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
#include "castor/server/NotificationThread.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"


// #define NOTIFY_PROTO "udp"         /* Notify Proto in /etc/services if any */
#define NOTIFY_PORT  "NOTIFYPORT"  /* Sub-label in config file for notification port */


namespace castor {

 namespace server {

  // Forward declaration
  class Mutex;
  class NotificationThread;

  /**
   * CASTOR thread pool supporting wakeup on signals
   * and periodical run after timeout.
   * Credits to Jean-Damien Durand for the underlying C code.
   */
  class SignalThreadPool : public BaseThreadPool {

  friend class NotificationThread;

  public:

    const static int NOTIFY_PORT_BASE = 65015;
    
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
                   const int notifPort = 0,
                   const int timeout = castor::server::Mutex::TIMEOUT);

    /*
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
    virtual void run();

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

    /**
     * Gets this thread pool's mutex.
     * used by threads for thread-safe operations.
     * @return a Mutex.
     */
    Mutex* getMutex() {
      return m_poolMutex;
    }

    /**
     * Get this thread pool's active threads count.
     * The function is thread-safe as counter is obtained
     * by locking the mutex.
     * @return int nbActiveThreads
     */
    int getActiveThreads()
      throw (castor::exception::Exception) {
      m_poolMutex->lock();
      return m_nbActiveThreads;
    }


  private:

    /// timeout between two subsequent wake ups of the underlying threads
    int m_timeout;

    /* Formerly struct singleService */
    int m_nbTotalThreads;
    int m_nbActiveThreads;
    int m_notified;
    bool m_notTheFirstTime;

    /// a mutex used by the threads to safely access this class' fields
    Mutex* m_poolMutex;

    /// UDP port where to listen for wake up signals
    int m_notifPort;

    /// the notification thread; we keep a pool only to reuse the already defined thread entrypoint.
    BaseThreadPool* m_notifTPool;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_SIGNALTHREADPOOL_HPP
