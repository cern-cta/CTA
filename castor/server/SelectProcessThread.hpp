/******************************************************************************
 *                      SelectProcessThread.hpp
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
 * Base thread for the select/process model: it loops until select() returns
 * something to do. If stop() is called, the underlying database connection is dropped.
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_SELECTPROCESSTHREAD_HPP
#define CASTOR_SERVER_SELECTPROCESSTHREAD_HPP 1

#include "castor/IObject.hpp"
#include "castor/BaseObject.hpp"
#include "castor/server/IThread.hpp"

namespace castor {

 namespace server {

  /**
   * Basic select/process thread for internal database-driven services.
   */
  class SelectProcessThread : public virtual castor::server::IThread,
                              public castor::BaseObject {
  public:

    /**
     * Select part of the service.
     * @return next operation to handle, 0 if none.
     */
    virtual castor::IObject* select() throw() = 0;

    /**
     * Process part of the service
     * @param param next operation to handle.
     */
    virtual void process(castor::IObject* param) throw() = 0;

    /**
     * Initialization of the thread.
     */
    virtual void init() {}

    /**
     * Main work for this thread
     */
    virtual void run(void* param);

    /**
     * Stop of the thread
     */
    virtual void stop() {}

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_SELECTPROCESSTHREAD_HPP
