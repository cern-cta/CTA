/******************************************************************************
 *                      DbAlertedThreadPool.hpp
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
 * Thread pool supporting wakeup from the DBMS_ALERT Oracle interface
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_DBALERTEDTHREADPOOL_HPP
#define CASTOR_SERVER_DBALERTEDTHREADPOOL_HPP 1

#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <iostream>
#include <string>

#include "osdep.h"
#include "serrno.h"

#include "castor/BaseObject.hpp"
#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/SelectProcessThread.hpp"
#include "castor/server/Mutex.hpp"
#include "castor/exception/Exception.hpp"


namespace castor {

 namespace server {

  // Forward declaration
  class SelectProcessThread;
  
  /**
   * CASTOR thread pool supporting wakeup from database
   */
  class DbAlertedThreadPool : public BaseThreadPool {

  public:

    /**
     * Empty constructor
     */
    DbAlertedThreadPool() :
      BaseThreadPool() {};

    /**
     * Constructor
     * @param poolName as in BaseThreadPool
     * @param thread the thread running the user code. It must
     *        be derived from SelectProcessThread, where
     *        the select phase waits for a db alert and the
     *        process phase executes the task.
     * @param nbThreads total number of threads in the pool
     */
    DbAlertedThreadPool(const std::string poolName,
                     castor::server::SelectProcessThread* thread,
                     const unsigned int nbThreads = DEFAULT_THREAD_NUMBER)
      throw (castor::exception::Exception);

    /**
     * Destructor
     */
    virtual ~DbAlertedThreadPool() throw();

    /**
     * Creates and runs the pool starting the threads in detached mode.
     */
    virtual void run() throw (castor::exception::Exception);

    /**
     * Shutdowns the pool.
     * @param wait flag to indicate to wait for all thread of this
     * pool to terminate.
     * @return true iff no thread is active (i.e. m_nbActiveThreads == 0).
     */
    virtual bool shutdown(bool wait = true) throw ();

  private:

    /// mutex used by the threads to safely access this class' fields
    pthread_mutex_t m_lock;

    /// thread attributes
    pthread_attr_t m_attr;

    /**
     * The start routine for threads in the pool.
     * @param arg A pointer to the thread pool object. This is needed so that
     * the threads can access the thread pools internal data.
     */
    static void* _runner(void* arg);

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_DBALERTEDTHREADPOOL_HPP
