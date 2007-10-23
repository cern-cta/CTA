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
 * @(#)$RCSfile: QueryRequestSvcThread.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/10/23 14:01:36 $ $Author: sponcec3 $
 *
 * Service thread for StageQueryRequest requests
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef STAGER_DAEMON_QUERYREQUESTSVCTHREAD_HPP
#define STAGER_DAEMON_QUERYREQUESTSVCTHREAD_HPP 1

#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/query/IQuerySvc.hpp"
#include "castor/server/SelectProcessThread.hpp"


namespace castor {

  namespace stager {

    namespace dbService {

      class QueryRequestSvcThread : public virtual castor::server::SelectProcessThread {

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
         * Select a new queryRequest to be processed
         */
        virtual castor::IObject* select() throw();

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
                                              Cuuid_t uuid)
          throw (castor::exception::Exception);
        /**
         * Handles a filequery by fileId and replies to client.
         */
        void handleFileQueryRequestByFileId(castor::query::IQuerySvc* qrySvc,
                                            castor::IClient *client,
                                            std::string &fid,
                                            std::string &nshost,
                                            u_signed64 svcClassId,
                                            std::string reqId,
                                            Cuuid_t uuid)
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
         * @param svcs the Services object to use
         * @param qrySvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         */
        void handleFileQueryRequest(castor::stager::Request* req,
                                    castor::IClient *client,
                                    castor::Services* svcs,
                                    castor::query::IQuerySvc* qrySvc,
                                    castor::BaseAddress &ad,
                                    Cuuid_t uuid)
          throw (castor::exception::Exception);

        /**
         * Handles a DiskPoolQuery and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param qrySvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         */
        void handleDiskPoolQuery(castor::stager::Request* req,
                                 castor::IClient *client,
                                 castor::Services* svcs,
                                 castor::query::IQuerySvc* qrySvc,
                                 castor::BaseAddress &ad,
                                 Cuuid_t uuid)
          throw (castor::exception::Exception);

        /**
         * Handles a VersionQuery and replies to client.
         * @param req the request to handle
         * @param client the client where to send the response
         */
        void handleVersionQuery(castor::stager::Request* req,
                                castor::IClient *client)
          throw (castor::exception::Exception);

        /**
         * helper method for cleaning up a request and releasing
         * a service if not null
         * @param req the request to clean
         * @param svc the service to release
         */
        void cleanup (castor::stager::Request* req,
                      castor::IService *svc) throw();

      };

    } // end namespace dbService

  } // end namespace stager

} //end namespace castor

#endif
