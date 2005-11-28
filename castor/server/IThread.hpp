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
 * @(#)$RCSfile: IThread.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/11/28 09:42:51 $ $Author: itglp $
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
   * Can be used within BaseThreadPool and SignalThreadPool.
   */
  class IThread {
  
  public:
    /**
     * Thread initialization.
     * @param param any relevant initialization value which
     * will be passed in by the pool.
     */
	virtual void init(void* param) = 0;
	
	/**
	 * Main work for this thread.
	 * This method is supposed to run a single "loop" of
	 * the job, so that 
	 */
	virtual void run() = 0;
	
	/**
	 * Convenience method to stop the thread.
	 * It is up to the inherited classes to implement
	 * the correct behaviour.
	 */
	virtual void stop() = 0;
	
	/**
	 * For infinite loop threads, gets the timeout
	 * until next wakeup for this thread.
	 * @return int a timeout in seconds.
	 */
	virtual int getTimeout() = 0;
  };
  
 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_ITHREAD_HPP
