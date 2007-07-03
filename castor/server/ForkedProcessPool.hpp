/******************************************************************************
 *                      ForkedProcessPool.hpp
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
 * @(#)$RCSfile: ForkedProcessPool.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/07/03 09:54:24 $ $Author: itglp $
 *
 * A pool of forked processes
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_FORKEDPROCESSPOOL_HPP
#define CASTOR_SERVER_FORKEDPROCESSPOOL_HPP 1

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


namespace castor {

 namespace server {

  // Forward declaration
  class Mutex;

  /**
   * CASTOR thread pool based on the fork() syscall
   */
  class ForkedProcessPool : public BaseThreadPool {

  public:

    /**
     * empty constructor
     */
     ForkedProcessPool() :
       BaseThreadPool(), m_poolMutex(0) {};

    /**
     * constructor
     */
    ForkedProcessPool(const std::string poolName,
                   castor::server::IThread* thread);
		   
    /*
     * destructor
     */
     virtual ~ForkedProcessPool() throw();

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
      return m_nbActiveThreads;
    }


  private:

    /* Formerly struct singleService */
    int m_nbTotalThreads;
    int m_nbActiveThreads;
    int m_notified;
    bool m_notTheFirstTime;

    /// a mutex used by the threads to safely access this class' fields
    Mutex* m_poolMutex;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_FORKEDPROCESSPOOL_HPP
