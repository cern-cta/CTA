/******************************************************************************
 *                castor/stager/daemon/LoggingThread.hpp
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
 * Thread for dumping database logs to DLF
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef STAGER_DAEMON_LOGGINGTHREAD_HPP
#define STAGER_DAEMON_LOGGINGTHREAD_HPP 1

#include "castor/IObject.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/IThread.hpp"

namespace castor {
  
  namespace stager {
    
    namespace daemon {
      
      class LoggingThread : public virtual castor::server::IThread,
                            public castor::BaseObject {
	
      public:

        /**
         * Initialization of the thread.
         */
        virtual void init() {}

        /**
         * Main work for this thread: dump the db logs to DLF
         */
        virtual void run(void* param) throw();

        /**
         * Stop of the thread
         */
        virtual void stop() {}

      };
      
    } // end namespace daemon
    
  } // end namespace stager
  
} //end namespace castor

#endif // STAGER_DAEMON_LOGGINGTHREAD_HPP
