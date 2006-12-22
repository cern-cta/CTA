/******************************************************************************
 *                      NotificationThread.hpp
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
 * @(#)$RCSfile: NotificationThread.hpp,v $ $Revision: 1.7 $ $Release$ $Date: 2006/12/22 17:24:15 $ $Author: sponcec3 $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_NOTIFICATIONTHREAD_HPP
#define CASTOR_SERVER_NOTIFICATIONTHREAD_HPP 1

#include <iostream>
#include <string>
#include "castor/IObject.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/exception/Exception.hpp"

#define MAX_BIND_RETRY 5


namespace castor {

 namespace server {

  class SignalThreadPool;
  
  /**
   * Notification thread for internal stager notifications.
   * This thread can handle infinite loops from user threads.
   */
  class NotificationThread : public virtual IThread {

  public:

    /// magic number for notification threads
    static const long NOTIFY_MAGIC = 0x44180876;

    /**
     * Initializes a notification thread.
     */
    NotificationThread(int notifPort);
    
    /**
     * Destructor
     */
    virtual ~NotificationThread() {};

    /**
     * Main work for this thread.
     */
    virtual void run(void* param);

    /**
  	 * The notification thread is never stopped.
     */
    virtual void stop() {};

  private:

    SignalThreadPool* m_owner;

    int m_notifPort;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_NOTIFICATIONTHREAD_HPP
