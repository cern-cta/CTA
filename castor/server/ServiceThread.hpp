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
 * @(#)$RCSfile: ServiceThread.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2006/01/16 10:09:48 $ $Author: itglp $
 *
 *
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
   * Service thread for internal stager services.
   * This thread can handle infinite loops from user threads.
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
     ~ServiceThread() throw();

    /**
     * Thread initialization.
     */
  	virtual void init(void* param);

  	/**
     * Main work for this thread.
  	 */
    virtual void run() throw();

    /**
     * Convenience method to stop the thread.
     * Simply delegates to the user thread.
     */
    virtual void stop() {
      m_userThread->stop();
    };

  private:

    SignalThreadPool* m_owner;
    
    IThread* m_userThread;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_SERVICETHREAD_HPP
