/******************************************************************************
 *                      NotifierThread.hpp
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
 * @(#)$RCSfile: NotifierThread.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2008/02/11 16:02:43 $ $Author: itglp $
 *
 * A thread to handle notifications to wake up workers in a pool
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_NOTIFIERTHREAD_HPP
#define CASTOR_SERVER_NOTIFIERTHREAD_HPP 1

#include <iostream>
#include <string>
#include "castor/IObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/BaseDaemon.hpp"

namespace castor {

 namespace server {
   
  // forward declaration
  class BaseDaemon;

  /**
   * Notification thread for internal daemon notifications.
   */
  class NotifierThread : public virtual IThread {

  public:

    /**
     * Initializes a notification thread.
     * @param owner the daemon which controls this thread
     */
    NotifierThread(castor::server::BaseDaemon* owner);
    
    /**
     * standard destructor
     */
    virtual ~NotifierThread() throw() {};
    
    /**
     * No initialization is needed for the notification thread.
     */
    virtual void init() {};

    /**
     * Main work for this thread.
     */
    virtual void run(void* param);

    /**
     * This thread can be stopped abruptly.
     */
    virtual void stop() {};
    
    /**
     * Performs the notification by signalling the appropriate 
     * condition variable
     */
    void doNotify(char tpName, int nbThreads = 1) throw ();

  private:

    /// the daemon which controls this notification thread
    BaseDaemon* m_owner;
    
  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_NOTIFIERTHREAD_HPP
