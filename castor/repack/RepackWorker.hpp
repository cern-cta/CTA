/******************************************************************************
 *                      RepackServerReqSvcThread.hpp
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
 * @(#)$RCSfile: RepackWorker.hpp,v $ $Revision: 1.29 $ $Release$ $Date: 2009/06/18 15:30:29 $ $Author: gtaur $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/

#ifndef REPACKWORKER_HPP
#define REPACKWORKER_HPP 1



#include "castor/server/IThread.hpp"
#include "RepackAck.hpp"
#include "RepackServer.hpp"
#include "RepackRequest.hpp"


namespace castor {

  namespace repack {
    class FileListHelper;
    class RepackServer;
    /**
     * A thread to process Put/Get request and submit them to the stager.
     */
    class RepackWorker : public castor::server::IThread {

    public:

      /**
       * Initializes the db access.
       */

      RepackWorker(RepackServer* pserver);


      /**
       * Standard destructor
       */

      ~RepackWorker() throw();

      virtual void init() {};

      virtual void run(void* param);
      virtual void stop() ;

    private:

      void  checkTapeVmgrStatus(std::string) throw (castor::exception::Exception);

      void  getPoolVmgrInfo(castor::repack::RepackRequest* rreq) throw (castor::exception::Exception);

      RepackAck* handleRepack(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception);

      RepackAck* removeRequest(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception);

      RepackAck* restartRequest(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception);

      RepackAck* archiveSubRequests(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception);

      RepackAck* archiveAllSubRequests(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception);

      RepackAck*  getSubRequestsWithSegments(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception);


      RepackAck* getStatusAll(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception);
      
      RepackAck* getStatus(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception);


      /**
       * The RepackServer instance pointer.
       */
      RepackServer* ptr_server;

    };

  } // end of namespace repack

} // end of namespace castor


#endif // REPACKWORKER_HPP
