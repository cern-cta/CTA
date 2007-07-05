/******************************************************************************
 *                      ForkedProcessPool.hpp
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
 * @(#)$RCSfile: ForkedProcessPool.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/07/05 18:10:29 $ $Author: itglp $
 *
 * A pool of forked processes
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_FORKEDPROCESSPOOL_HPP
#define CASTOR_SERVER_FORKEDPROCESSPOOL_HPP 1

/* ============== */
/* System headers */
/* ============== */
#include <errno.h>                      /* For EINVAL etc... */
#include "osdep.h"
#include <sys/types.h>
#include <iostream>
#include <string>
#include <vector>

/* ============= */
/* Local headers */
/* ============= */
#include "serrno.h"                     /* CASTOR error numbers */

#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/IThread.hpp"
#include "castor/io/PipeSocket.hpp"
#include "castor/exception/Exception.hpp"


namespace castor {

 namespace server {

  /**
   * CASTOR thread pool based on the fork() syscall
   * and using pipes for inter-process communication
   */
  class ForkedProcessPool : public BaseThreadPool {

  public:

    /**
     * empty constructor
     */
    ForkedProcessPool() :
      BaseThreadPool() {};

    /**
     * constructor
     */
    ForkedProcessPool(const std::string poolName,
                      castor::server::IThread* thread);
		   
    /*
     * destructor
     */
    virtual ~ForkedProcessPool() throw();

    /**
     * Initializes the pool
     */
    virtual void init() throw (castor::exception::Exception);

    /**
     * Creates and runs the pool by forking children processes
     */
    virtual void run();
    
    /**
     * Entry point to dispatch a task to an idle process
     */
    virtual void dispatch(castor::IObject& obj);
    
  private:

    /**
     * The main loop of the children processes
     */
    void childRun();

    /**
     * A vector of PipeSockets to communicate to the children processes
     */
    std::vector<castor::io::PipeSocket*> childPipe;
  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_FORKEDPROCESSPOOL_HPP
