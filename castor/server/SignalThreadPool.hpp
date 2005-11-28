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
 * @(#)$RCSfile: SignalThreadPool.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/11/28 09:42:51 $ $Author: itglp $
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
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"


namespace castor {

 namespace server {

  // FOrward declaration
  class Mutex;

  static int DEFAULT_NOTIFY_PORT = 65015;

  static long NOTIFY_MAGIC = 0x44140701;

  /**
   * CASTOR thread pool supporting wakeup on signals
   * and periodical run after timeout.
   */
  class SignalThreadPool : public BaseThreadPool {

  public:

    /**
     * empty constructor
     */
     SignalThreadPool() throw() :
       BaseThreadPool(), m_poolMutex(0) {};

    /**
     * constructor
     */
    SignalThreadPool(const std::string poolName,
                   castor::server::IThread* thread,
                   const int nbThreads = DEFAULT_THREAD_NUMBER,
                   const int notifyPort = DEFAULT_NOTIFY_PORT) throw();

    /*
     * destructor
     */
     virtual ~SignalThreadPool() throw();

    /**
     * Initializes the pool
     */
    virtual void init() throw (castor::exception::Exception);

    /**
     * Create and run the pool starting the threads in detached mode.
     * XXX later: starts the notification thread for this pool.
     */
    virtual void run();

    /**
     * Assigns work to a thread from the pool.
     * In this implementation we have to override the fork.
     * XXX To be reviewed later...
     */
    virtual int threadAssign(void *param) {};


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


  private:

    /// timeout between two subsequent wake ups of the underlying threads
    int m_timeout;

    /// UDP port where to listen for wake up signals
    int m_notifyPort;

    /* Formerly struct singleService */
    int m_nbTotalThreads;
    int m_nbActiveThreads;
    int m_nbNotifyThreads;
    int m_notified;
    bool m_notTheFirstTime;

    Mutex* m_poolMutex;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_BASETHREADPOOL_HPP
