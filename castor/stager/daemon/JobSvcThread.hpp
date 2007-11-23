/******************************************************************************
 *                castor/stager/daemon/JobSvcThread.hpp
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
 * @(#)$RCSfile: JobSvcThread.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2007/11/23 11:27:45 $ $Author: sponcec3 $
 *
 * Service thread for job related requests
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef STAGER_DAEMON_JOBSVCTHREAD_HPP
#define STAGER_DAEMON_JOBSVCTHREAD_HPP 1

#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/SelectProcessThread.hpp"
#include "castor/stager/IJobSvc.hpp"

namespace castor {

  namespace stager {

    namespace dbService {

      class JobSvcThread : public virtual castor::server::SelectProcessThread {

      public:

        /**
         * Default costructor
         */
        JobSvcThread() throw();

        /**
         * Default destructor
         */
        ~JobSvcThread() throw() {};

        /**
         * Select a new job request to be processed
         */
        virtual castor::IObject* select() throw();

        /**
         * Processes a request
         * @param param The IObject returned by select
         */
        virtual void process(castor::IObject* param) throw();

      private:

        /**
         * Handles a StartRequest and replies to the client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param jobSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleStartRequest(castor::stager::Request* req,
                                castor::IClient *client,
                                castor::Services* svcs,
                                castor::stager::IJobSvc* jobSvc,
                                castor::BaseAddress &ad,
                                Cuuid_t uuid) throw();

        /**
         * Handles a Disk2DiskCopyStartRequest and reples to the client.
         * If an exception occurs the call will attempt to fail the
         * disk2disk copy transfer on behalf of the client
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param jobSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleDisk2DiskCopyStartRequest(castor::stager::Request* req,
                                             castor::IClient *client,
                                             castor::Services* svcs,
                                             castor::stager::IJobSvc* jobSvc,
                                             castor::BaseAddress &ad,
                                             Cuuid_t uuid) throw();

        /**
         * Handles a Disk2DiskCopyDoneRequest and replies to the client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param jobSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleDisk2DiskCopyDoneRequest(castor::stager::Request* req,
                                            castor::IClient *client,
                                            castor::Services* svcs,
                                            castor::stager::IJobSvc* jobSvc,
                                            castor::BaseAddress &ad,
                                            Cuuid_t uuid) throw();

        /**
         * Handles a MoverCloseRequest and replies to the client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param jobSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleMoverCloseRequest(castor::stager::Request* req,
                                     castor::IClient *client,
                                     castor::Services* svcs,
                                     castor::stager::IJobSvc* jobSvc,
                                     castor::BaseAddress &ad,
                                     Cuuid_t uuid) throw();

        /**
         * Handles a GetUpdateDone request and replies to the client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param jobSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleGetUpdateDoneRequest(castor::stager::Request* req,
                                        castor::IClient *client,
                                        castor::Services* svcs,
                                        castor::stager::IJobSvc* jobSvc,
                                        castor::BaseAddress &ad,
                                        Cuuid_t uuid) throw();

        /**
         * Handles a GetUpdateFailed request and replies to the client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param jobSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleGetUpdateFailedRequest(castor::stager::Request* req,
                                          castor::IClient *client,
                                          castor::Services* svcs,
                                          castor::stager::IJobSvc* jobSvc,
                                          castor::BaseAddress &ad,
                                          Cuuid_t uuid) throw();

        /**
         * Handles a PutFailed request and replies to the client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param jobSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handlePutFailedRequest(castor::stager::Request* req,
                                    castor::IClient *client,
                                    castor::Services* svcs,
                                    castor::stager::IJobSvc* jobSvc,
                                    castor::BaseAddress &ad,
                                    Cuuid_t uuid) throw();

        /**
         * Handles a FirstByteWritten request and replies to the client.
         * @param req the request to handle
         * @param client the client where to send the response
         * @param svcs the Services object to use
         * @param jobSvc the stager service to use
         * @param ad the address where to load/store objects in the DB
         * @param uuid the uuid of the request, for logging purposes
         */
        void handleFirstByteWrittenRequest(castor::stager::Request* req,
                                           castor::IClient *client,
                                           castor::Services* svcs,
                                           castor::stager::IJobSvc* jobSvc,
                                           castor::BaseAddress &ad,
                                           Cuuid_t uuid) throw();

      };

    } // end namespace dbService

  } // end namespace stager

} //end namespace castor

#endif
