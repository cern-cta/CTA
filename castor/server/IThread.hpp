/******************************************************************************
 *                      IThread.hpp
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
 * @(#)$RCSfile: IThread.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2006/02/20 14:37:32 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_ITHREAD_HPP
#define CASTOR_SERVER_ITHREAD_HPP 1

#include <iostream>
#include <string>
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"

namespace castor {

 namespace server {

  /**
   * Basic CASTOR thread interface.
   * Can be used within ListenerThreadPool and SignalThreadPool.
   */
  class IThread {
  
  public:
	
    /**
     * Main work for this thread.
     * This method will run on a dedicated thread, so it may
     * contain an infinite loop; however, it is advised to let it
     * run a chunk of your job and let the pool rerun it when
     * needed (either a new request coming for ListenerThreadPools
     * or timeout elapsed/notification got for SignalThreadPools).
     * @param param any relevant initialization value which
     * will be passed in by the pool.
     * @throw any exception thrown here is handled in the BaseThreadPool. 
     */
    virtual void run(void *param) = 0;
    
    /**
     * Convenience method to stop the thread.
     * It is up to the inherited classes to implement
     * the expected behaviour.
     */
    virtual void stop() = 0;
	
  };
  
 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_ITHREAD_HPP
