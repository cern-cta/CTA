/******************************************************************************
 *                      BaseDbThread.hpp
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
 * @(#)$RCSfile: BaseDbThread.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/07/09 17:06:01 $ $Author: itglp $
 *
 * Base class for a database oriented thread. It correctly implements the stop
 * method, but it can be used only for a pool with a single thread.
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_BASEDBTHREAD_HPP
#define CASTOR_SERVER_BASEDBTHREAD_HPP 1

#include <iostream>
#include <string>
#include "castor/server/IThread.hpp"
#include "castor/BaseObject.hpp"
#include "castor/db/DbCnvSvc.hpp"

namespace castor {

 namespace server {

  /**
   * Basic select/process thread for internal stager services.
   */
  class BaseDbThread : public virtual IThread, public castor::BaseObject {
  public:

    /**
     * Empty constructor
     */
    BaseDbThread() {};

    /**
     * Empty init method
     */
    virtual void init() {};

    /**
     * Main work for this thread. Creates a db connection
     */
    virtual void run(void* param);

    /**
     * Stops the thread and drops its db connection
     */
    virtual void stop();
    
  private:
    /** pointer to the db conversion service. This is shared,
     * and it is used inside stop() to interrupt this thread's activity.
     * Note that a thread pool running this thread must have nbThreads = 1
     * to be thread-safe.
     */
    castor::db::DbCnvSvc* m_cnvSvc;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_SELECTPROCESSTHREAD_HPP
