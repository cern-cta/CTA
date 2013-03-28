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
 *
 * A pool of forked processes
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_FORKEDPROCESSPOOL_HPP
#define CASTOR_SERVER_FORKEDPROCESSPOOL_HPP 1

#include <vector>

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
       * constructor
       */
      ForkedProcessPool(const std::string poolName,
        castor::server::IThread* thread)
        throw (castor::exception::Exception);
      
      /*
       * destructor
       */
      virtual ~ForkedProcessPool() throw();
      
      /**
       * Creates the pool by forking children processes.
       * The parent process returns, children processes call childRun.
       */
      virtual void init() throw (castor::exception::Exception);
      
      /**
       * Shutdowns the pool. Kills all children.
       */
      virtual bool shutdown(bool wait) throw ();
      
      /**
       * Runs the pool. The forking phase is done in init, thus this
       * method is almost empty.
       */
      virtual void run() throw (castor::exception::Exception);
      
      /**
       * Entry point to dispatch a task to an idle process
       * @throw exception in case either select() or the transmission
       * through the pipe fails.
       */
      void dispatch(castor::IObject& obj)
        throw (castor::exception::Exception);
      
    protected:
      
      /**
       * The main loop of the children processes. This method is supposed
       * to run forever or until the pipe is not broken.
       */
      virtual void childRun(castor::io::PipeSocket* ps);
      
      /// The vector of PipeSockets to communicate to the children processes
      std::vector<castor::io::PipeSocket*> m_childPipe;
      
      /// The process id of the child processes
      int *m_childPid;
      
      /// The set of pipes to the children for writing, to be used with select
      fd_set m_writePipes;
      
      /// The highest numbered writing file descriptor
      int m_writeHighFd;
            
    };
    
  } // end of namespace server
  
} // end of namespace castor


#endif // CASTOR_SERVER_FORKEDPROCESSPOOL_HPP
