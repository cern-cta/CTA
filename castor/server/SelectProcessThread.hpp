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
 * @(#)$RCSfile: SelectProcessThread.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2006/02/01 17:11:47 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_SELECTPROCESSTHREAD_HPP
#define CASTOR_SERVER_SELECTPROCESSTHREAD_HPP 1

#include <iostream>
#include <string>
#include "castor/IObject.hpp"
#include "castor/server/ServiceThread.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

 namespace server {

  /**
   * Basic select/process thread for internal stager services.
   */
  class SelectProcessThread : public virtual IThread {
  public:

    /**
  	 * Initializes a service thread.
  	 */
  	SelectProcessThread() : stopped(false) {};

    /**
  	 * Select part of the service
     */
  	virtual castor::IObject* select() = 0;

  	/**
	   * Process part of the service
     */
  	virtual void process(castor::IObject* param) = 0;

  	/**
     * Main work for this thread.
  	 */
    virtual void run(void* param) throw() {
      while(!stopped) {
        castor::IObject* selectOutput = select();
        if(selectOutput == 0) break;
        process(selectOutput);
      }
    };

    /**
     * Convenience method to stop the thread.
     */
    virtual void stop() {
      stopped = true;
    };
    
  private:
    bool stopped;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_SELECTPROCESSTHREAD_HPP
