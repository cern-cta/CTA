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
 * @(#)$RCSfile: BaseThreadPool.hpp,v $ $Revision: 1.19 $ $Release$ $Date: 2009/01/08 09:24:24 $ $Author: itglp $
 *
 * Abstract CASTOR thread pool
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
   * Default number of threads in a pool
   */
  static const int DEFAULT_THREAD_NUMBER = 20;

  /**
   * Basic CASTOR thread pool. Should not be instantiated by
   * user applications; other pools provide full functionality to run threads.
   */
  class BaseThreadPool : public BaseObject {

  public:

    /**
     * Empty constructor
     */
    BaseThreadPool() : 
       BaseObject(), m_poolName(""), m_thread(0) {};

    /**
     * Constructor
     * @param poolName the name of this pool
     * @param thread the user code to be run
     * @param nbThreads the number of threads to be executed
     */
    BaseThreadPool(const std::string poolName,
                   castor::server::IThread* thread,
                   unsigned int nbThreads = DEFAULT_THREAD_NUMBER)
      throw (castor::exception::Exception);

    /*
     * Destructor
     */
    virtual ~BaseThreadPool() throw();

    /**
     * Initializes the pool. This function is called before
     * any forking may take place.
     */
    virtual void init() throw (castor::exception::Exception);

    /**
     * Runs the pool. This function is supposed to spawn
     * the threads and immediately return.
     * Specialized pools implement it according to their needs,
     * this implementation throws an exception.
     */
    virtual void run() throw (castor::exception::Exception);
    
    /**
     * Performs a graceful shutdown of the pool. This method is
     * supposed to asynchronously signal all threads in the pool 
     * in order to stop them. This implementation always returns
     * true, ignoring the argument.
     * @param wait if true, the call blocks until all threads
     * are over.
     * @return true if the pool is idle, false if it is not and
     * the wait argument was false: in the second case, the
     * caller is supposed to wait and try again later.
     */
    virtual bool shutdown(bool wait = false) throw();
     
    /**
     * Sets the number of threads
     */
    virtual void setNbThreads(unsigned int value);

    /**
     * Gets the underlying worker thread
     * @return pointer to the thread
     */
    castor::server::IThread* getThread() {
      return m_thread;
    }

    /**
     * Gets the pool name initial as identifier for this pool
     */
    char getPoolId() {
      return m_poolName[0];
    }
    
    /**
     * Gets the pool name
     */
    const std::string getName() {
      return m_poolName;
    }
    
    /**
     * Gets the number of threads belonging to this pool
     */
    unsigned int getNbThreads() {
      return m_nbThreads;
    }

    /**
     * Tells whether the pool has been terminated
     * @return the value of the m_stopped flag
     */
    bool stopped() {
      return m_stopped;
    }
    
    /**
     * Resets internal counters for calculating monitoring metrics
     */
    virtual void resetMetrics();
    
    /**
     * Pointer to a generic method returning a monitoring metric
     */
    typedef u_signed64 (BaseThreadPool::*MetricGetter)();
    
    /**
     * Methods returning internal monitoring metrics
     */
    u_signed64 getAvgTaskTime() {
      // average time to process a task in ms
      return (u_signed64)(m_runsCount > 0 ?
        m_activeTime * 1000 / m_runsCount : 0);
    }
     
    virtual u_signed64 getActivityFactor() {
      // percentage value in the range 0..100
      return (u_signed64)(m_activeTime + m_idleTime > 0 ?
        m_activeTime * 100 / (m_activeTime + m_idleTime) : 0);
    }
    
    virtual u_signed64 getLoadFactor() {
      // percentage value in the range 0..100
      return (u_signed64)(m_nbThreads > 0 ?
        m_nbActiveThreads * 100.0 / m_nbThreads : 0);
    }
    
    virtual u_signed64 getBacklogFactor() {
      // placeholder, DynamicThreadPool implements it
      return 0;
    }
    
    virtual u_signed64 getAvgQueuingTime() {
      // placeholder, DynamicThreadPool implements it
      return 0;
    }

  protected:

    /// Number of threads in the pool
    unsigned int m_nbThreads;

    /// Name of the pool
    std::string m_poolName;

    /// The worker thread
    IThread* m_thread;
    
    /// Flag to indicate whether the thread pool is stopped
    bool m_stopped;
    
    /// count of the current number of busy threads in the pool
    unsigned int m_nbActiveThreads;
    
    /// time counters to measure total idle and active time
    /// of all threads in this pool
    double m_idleTime, m_activeTime;
    
    /// total count of the executed runs for all threads in the pool
    u_signed64 m_runsCount;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_BASETHREADPOOL_HPP
