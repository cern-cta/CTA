/******************************************************************************
 *                castor/stager/daemon/BulkStageReqSvcThread.hpp
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
 * @(#)$RCSfile: BulkStageReqSvcThread.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2008/01/15 14:50:46 $ $Author: itglp $
 *
 * Service thread for bulk requests
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef STAGER_DAEMON_BULKSTAGEREQSVCTHREAD_HPP
#define STAGER_DAEMON_BULKSTAGEREQSVCTHREAD_HPP 1

#include "castor/IObject.hpp"
#include "castor/server/SelectProcessThread.hpp"

namespace castor {

  namespace stager {

    namespace daemon {

      class BulkStageReqSvcThread : public virtual castor::server::SelectProcessThread {

      public:

        /**
         * Default costructor
         */
        BulkStageReqSvcThread() throw();

        /**
         * Default destructor
         */
        ~BulkStageReqSvcThread() throw() {};
        
        /**
         * Select part of the service.
         * For bulk request, this function actually handles the request to the end
         * (including its deletion from the DB). The process part will only serve
         * the purpose of logging and anwering the client
         * @return next operation to handle, 0 if none.
         */
        virtual castor::IObject* select() throw();

        /**
         * Processes a request
         * For bulk requests, this function only logs and answers the client.
         * The processing is entirely done in the select method.
         * @param param The IObject returned by select
         */
        virtual void process(castor::IObject* param) throw();
      };

    } // end namespace daemon

  } // end namespace stager

} //end namespace castor

#endif
