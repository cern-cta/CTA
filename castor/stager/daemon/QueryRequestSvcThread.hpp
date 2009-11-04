/******************************************************************************
 *                castor/stager/daemon/QueryRequestSvcThread.hpp
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
 * @(#)$RCSfile: QueryRequestSvcThread.hpp,v $ $Revision: 1.10 $ $Release$ $Date: 2009/05/18 13:42:52 $ $Author: waldron $
 *
 * Service thread for StageQueryRequest requests
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef STAGER_DAEMON_QUERYREQUESTSVCTHREAD_HPP
#define STAGER_DAEMON_QUERYREQUESTSVCTHREAD_HPP 1

#include "castor/stager/daemon/BaseRequestSvcThread.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/RequestQueryType.hpp"

namespace castor {

  // Forward declarations
  class IObject;
  class IClient;
  namespace query {
    class IQuerySvc;
  }
  namespace rh {
    class IRHSvc;
    class FileQryResponse;
  }

  namespace stager {

    // Forward declarations
    class DiskCopyInfo;

    namespace daemon {

      class QueryRequestSvcThread : public virtual BaseRequestSvcThread {

      public:

        /**
         * Default costructor
         */
        QueryRequestSvcThread() throw();

        /**
         * Default destructor
         */
        ~QueryRequestSvcThread() throw() {};

        /**
         * Performs the selected query
         * @param param The IObject returned by select
         */
        virtual void process(castor::IObject* param) throw();

      private:

        /**
         * defines the status of a fileResponse
         */
        void setFileResponseStatus(castor::rh::FileQryResponse* fr,
                                   castor::stager::DiskCopyInfo* dc,
                                   bool& foundDiskCopy) throw();

        /**
         * Handles a filequery by fileId and replies to client.
         */
        void handleFileQueryRequestByFileName(castor::query::IQuerySvc* qrySvc,
                                              castor::IClient *client,
                                              std::string& fileName,
                                              u_signed64 svcClassId,
                                              std::string reqId,
                                              Cuuid_t uuid,
                                              bool all)
          throw (castor::exception::Exception);

        /**
         * Handles a filequery by fileId and replies to client.
         */
        void handleFileQueryRequestByFileId(castor::query::IQuerySvc* qrySvc,
                                            castor::IClient *client,
                                            u_signed64 fid,
                                            std::string &nshost,
                                            std::string& fileName,
                                            u_signed64 svcClassId,
                                            std::string reqId,
                                            Cuuid_t uuid,
                                            bool all)
          throw (castor::exception::Exception);

        /**
         * Handles a filequery by reqId/userTag or getLastRecalls version and replies to client.
         */
        void handleFileQueryRequestByRequest(castor::query::IQuerySvc* qrySvc,
                                             castor::IClient *client,
                                             castor::stager::RequestQueryType reqType,
                                             std::string &val,
                                             u_signed64 svcClassId,
                                             std::string reqId,
                                             Cuuid_t uuid)
          throw (castor::exception::Exception);

        /**
         * Handles a fileQueryRequest and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param qrySvc the stager service to use
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleFileQueryRequest(castor::stager::Request* req,
                                    castor::IClient *client,
                                    castor::query::IQuerySvc* qrySvc,
                                    Cuuid_t uuid)
          throw (castor::exception::Exception);

        /**
         * Handles a DiskPoolQuery and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param qrySvc the stager service to use
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleDiskPoolQuery(castor::stager::Request* req,
                                 castor::IClient *client,
                                 castor::query::IQuerySvc* qrySvc,
                                 Cuuid_t uuid)
          throw (castor::exception::Exception);

        /**
         * Handles a ChangePrivilege Request and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param rhSvc the RH service to use
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleChangePrivilege(castor::stager::Request* req,
                                   castor::IClient *client,
                                   castor::rh::IRHSvc* rhSvc,
                                   Cuuid_t uuid)
          throw (castor::exception::Exception);

        /**
         * Handles a ListPrivilege Request and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param rhSvc the RH service to use
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleListPrivileges(castor::stager::Request* req,
                                  castor::IClient *client,
                                  castor::rh::IRHSvc* rhSvc,
                                  Cuuid_t uuid)
          throw (castor::exception::Exception);

        /**
         * Handles a VersionQuery and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleVersionQuery(castor::stager::Request* req,
                                castor::IClient *client,
                                Cuuid_t uuid)
          throw (castor::exception::Exception);

        /**
         * helper method for cleaning up a request and releasing
         * services (only if not null)
         * @param req the request to clean
         * @param svc a service to release
         * @param extraSvc another service to release
         */
        void cleanup (castor::stager::Request* req,
                      castor::IService *svc,
                      castor::IService *extraSvc) throw();

      };

    } // end namespace daemon

  } // end namespace stager

} //end namespace castor

#endif
