/******************************************************************************
 *                      BaseThreadPool.hpp
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
 * @(#)$RCSfile: BaseThreadPool.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2005/12/07 17:11:58 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_BASETHREADPOOL_HPP
#define CASTOR_SERVER_BASETHREADPOOL_HPP 1

#include <iostream>
#include <string>
#include "castor/server/IThread.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"

namespace castor {

 namespace server {

  /**
   * default number of threads in the pool
   */
  static const int DEFAULT_THREAD_NUMBER = 20;

  /**
   * Basic CASTOR thread pool.
   */
  class BaseThreadPool : public BaseObject {

  public:

    /**
     * empty constructor
     */
    BaseThreadPool() throw() : 
       BaseObject(), m_thread(0), m_poolName("") {};

    /**
     * constructor
     */
    BaseThreadPool(const std::string poolName,
                   castor::server::IThread* thread) throw();

    /*
     * destructor
     */
    virtual ~BaseThreadPool() throw();

    /**
     * Initializes the pool
     */
    virtual void init() throw (castor::exception::Exception);

    /**
     * User function to run the pool.
     * Default implementation forks a single thread by
     * calling threadAssign(0).
     */
    virtual void run();

    /**
     * Forks and assigns work to a thread from the pool.
     * @param param user parameter passed to thread->init().
     */
    virtual int threadAssign(void *param);

    /**
     * Sets the foreground flag
     */
    void setForeground(bool value);

    /**
     * Sets the number of threads
     */
    void setNbThreads(int value);

    /**
     * Gets the message service log stream
     * Note that the service has to be released after usage
     * @return a pointer to the message service or 0 if none
     * is available.
     */
    std::ostream& log() throw (castor::exception::Exception);


    /**
     * Gets the underlying working thread
     * @return pointer to the thread
     */
    castor::server::IThread* getThread() {
      return m_thread;
    }

    /**
     * Gets the the pool name initial as identifier for this pool
     */
    const char getPoolId() {
      return m_poolName[0];
    }

    /**
     * Gets a pointer in thread local storage
     * Returns 0 if the void * could be allocated
     * -1 otherwise.
     * XXX TODO to be made accessible from threads?
     */
    //static int gettls(void **thip);

  protected:

    /**
     * Flag indicating whether the server should
     * run in foreground or background mode.
     */
    bool m_foreground;

    /**
     * The id of the pool created
     */
    int m_threadPoolId;

    /**
     * Number of threads in the pool.
     * If 1, in this pool the Cthread API will not 
     * be used and the user thread will be run directly.
     * XXX to be reviewed since multiple thread pools wouldn't work in this case!
     * See also the SignalThreadPool, which always runs
     * multithreaded.
     */
    int m_nbThreads;

    /**
     * Name of the pool
     */
    std::string m_poolName;

    /**
     * The working thread
     */
    IThread* m_thread;
    
    /// Thread entrypoint made friend to access private fields.
    friend void* _thread_run(void* param);

  };

/**
 * External entrypoint for the thread.
 * It is passed to Cpool_assign.
 */
 void* _thread_run(void* param);

 } // end of namespace server

} // end of namespace castor

/**
 * Structure used to pass arguments to the thread through Cpool_assign
 */
struct threadArgs {
  castor::server::BaseThreadPool *handler;
  void *param;
};


#endif // CASTOR_SERVER_BASETHREADPOOL_HPP
