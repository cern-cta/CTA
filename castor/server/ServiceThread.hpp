/******************************************************************************
 *                      ServiceThread.hpp
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
 * @(#)$RCSfile: ServiceThread.hpp,v $ $Revision: 1.9 $ $Release$ $Date: 2008/04/15 07:42:35 $ $Author: murrayc3 $
 *
 * Internal thread to allow user service threads running forever
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_SERVICETHREAD_HPP
#define CASTOR_SERVER_SERVICETHREAD_HPP 1

#include <iostream>
#include <string>
#include "castor/IObject.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

 namespace server {

  /**
   * Internal service thread for daemonized services running forever.
   * Original code in stager.c.
   */
  class ServiceThread : public IThread {

  public:

    /**
     * Initializes a service thread.
     * @param userThread the thread class containing the user code to be executed.
     */
    ServiceThread(IThread* userThread);
    
    /**
     * Destructor
     */
    virtual ~ServiceThread() throw();

    /**
     * Convenience method to initialize the thread.
     * Simply delegates to the user thread.
     */
    virtual void init() {
      m_userThread->init();
    };

    /**
     * Main work for this thread.
     */
    virtual void run(void* param);

    /**
     * Convenience method to stop the thread.
     */
    virtual void stop();
    
    /**
     * 
     */
    bool stopped() {
      return m_stopped;
    };

  private:

    /// the thread pool owning this thread
    SignalThreadPool* m_owner;
    
    /// flag to signal that the thread has to stop
    bool m_stopped;
    
    /// pointer to the class containing the user code to be executed
    IThread* m_userThread;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_SERVICETHREAD_HPP
