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
 * @(#)$RCSfile: BaseDbThread.hpp,v $ $Revision: 1.7 $ $Release$ $Date: 2009/01/08 09:28:51 $ $Author: itglp $
 *
 * Base class for a database oriented thread. It correctly implements the stop
 * method by dropping the db connection for each thread in the pool.
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_BASEDBTHREAD_HPP
#define CASTOR_SERVER_BASEDBTHREAD_HPP 1

#include <iostream>
#include <string>
#include "castor/server/IThread.hpp"
#include "castor/BaseObject.hpp"

namespace castor {

 namespace server {

  /**
   * Basic abstract db thread which drops the db connection upon stop
   */
  class BaseDbThread : public virtual IThread, 
                       public castor::BaseObject {
  
  public:

    /**
     * Empty constructor
     */
    BaseDbThread() : BaseObject() {};
    
    /**
     * Init method. Creates a thread-specific db connection
     */
    virtual void init();

    /**
     * Stops the thread and drops its db connection
     */
    virtual void stop();

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_SELECTPROCESSTHREAD_HPP
