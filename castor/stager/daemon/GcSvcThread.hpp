/******************************************************************************
 *                castor/stager/daemon/GcSvcThread.hpp
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
 * @(#)$RCSfile: GcSvcThread.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2007/11/20 17:20:35 $ $Author: sponcec3 $
 *
 * Service thread for garbage collection related requests
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef STAGER_DAEMON_GCSVCTHREAD_HPP
#define STAGER_DAEMON_GCSVCTHREAD_HPP 1

#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/SelectProcessThread.hpp"
#include "castor/stager/IGCSvc.hpp"

namespace castor {
  
  namespace stager {
    
    namespace dbService {
      
      class GcSvcThread : public virtual castor::server::SelectProcessThread {
	
      public:
      
        /**
         * Default costructor
         */
        GcSvcThread() throw();
       
        /**
         * Default destructor
         */
        ~GcSvcThread() throw() {};

        /**
         * Select a new gc request to be processed
         */
        virtual castor::IObject* select() throw();

        /**
         * Processes a request
         * @param param The IObject returned by select
         */
        virtual void process(castor::IObject* param) throw();

      private:

        /**
         * Handles FilesDeleted and FilesDeletionFailed requests
         * and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param gcSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleFilesDeletedOrFailed (castor::stager::Request* req,
                                         castor::IClient *client,
                                         castor::Services* svcs,
                                         castor::stager::IGCSvc* gcSvc,
                                         castor::BaseAddress &ad,
                                         Cuuid_t uuid) throw();

        /**
         * Handles a Files2Delete request and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param gcSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleFiles2Delete(castor::stager::Request* req,
                                castor::IClient *client,
                                castor::Services* svcs,
                                castor::stager::IGCSvc* gcSvc,
                                castor::BaseAddress &ad,
                                Cuuid_t uuid) throw();

        /**
         * Handles a NsFilesDelete request and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param gcSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleNsFilesDeleted(castor::stager::Request* req,
				  castor::IClient *client,
				  castor::Services* svcs,
				  castor::stager::IGCSvc* gcSvc,
				  castor::BaseAddress &ad,
				  Cuuid_t uuid) throw();

      };
      
    } // end namespace dbService
    
  } // end namespace stager
  
} //end namespace castor

#endif
