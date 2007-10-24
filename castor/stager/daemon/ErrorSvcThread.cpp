/******************************************************************************
 *                castor/stager/daemon/ErrorRequestSvcThread.cpp
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
 * @(#)$RCSfile: ErrorSvcThread.cpp,v $ $Revision: 1.15 $ $Release$ $Date: 2007/10/24 09:17:12 $ $Author: sponcec3 $
 *
 * Service thread for dealing with requests that failed
 *
 * @author castor dev team
 *****************************************************************************/

#include "Cthread_api.h"
#include "Cmutex.h"

#include <sstream>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"
#include "castor/stager/dbService/ErrorSvcThread.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::dbService::ErrorSvcThread::ErrorSvcThread() throw () {}

//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::stager::dbService::ErrorSvcThread::select()
  throw() {
  try {
    // get the StagerSvc. Note that we cannot cache it since we
    // would not be thread safe
    castor::Services *svcs = castor::BaseObject::services();
    castor::IService *svc = svcs->service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
    castor::stager::IStagerSvc *stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);
    if (0 == stgSvc) {
      // "Could not get StagerSvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("function", "ErrorSvcThread::select")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_ERRSVC_GETSVC, 1, params);
      return 0;
    }
    // actual work
    castor::stager::SubRequest* subReq = stgSvc->subRequestFailedToDo();
    stgSvc->release();
    return subReq;
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "ErrorSvcThread::select"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_ERRSVC_EXCEPT, 1, params);
    return 0;
  }
}

//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::dbService::ErrorSvcThread::process
(castor::IObject *param) throw() {
  // Useful variables
  castor::stager::Request* req = 0;
  castor::stager::SubRequest* subReq = 0;
  castor::Services *svcs = 0;
  castor::stager::IStagerSvc *stgSvc = 0;
  Cuuid_t suuid = nullCuuid;
  Cuuid_t uuid = nullCuuid;
  castor::IClient *client = 0;
  // address to access db
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  try {
    // get the StagerSvc. Note that we cannot cache it since we
    // would not be thread safe
    svcs = castor::BaseObject::services();
    castor::IService* svc = svcs->service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
    stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);
    if (0 == stgSvc) {
      // "Could not get StagerSvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("function", "ErrorSvcThread::process")};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_ERRSVC_GETSVC, 1, params);
      return;
    }
    // Retrieving subrequest, request and client from the
    // database. Note that casting the subrequest will
    // never be null since select returns one for sure
    subReq = dynamic_cast<castor::stager::SubRequest*>(param);
    string2Cuuid(&suuid, (char*)subReq->subreqId().c_str());
    // Getting the request
    svcs->fillObj(&ad, subReq, castor::OBJ_FileRequest);
    req = subReq->request();
    if (0 == req) {
      // "No request associated with subrequest ! Cannot answer !"
      castor::dlf::Param params[] = {castor::dlf::Param(suuid)};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_ERRSVC_NOREQ, 1, params);
    } else {
      string2Cuuid(&uuid, (char*)req->reqId().c_str());
      // Getting the client
      svcs->fillObj(&ad, req, castor::OBJ_IClient);
      client = req->client();
      if (0 == client) {
        // "No client associated with request ! Cannot answer !"
        castor::dlf::Param params[] = {castor::dlf::Param(suuid)};
        castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_ERRSVC_NOCLI, 1, params);
      }
    }
  } catch (castor::exception::Exception e) {
    // If we fail here, we do NOT have enough information
    // to reply to the client ! So we only log something.
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "ErrorSvcThread::process.1"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code()),
       castor::dlf::Param(suuid)};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_ERRSVC_EXCEPT, 4, params);
  }

  // At this point we are able to reply to the client
  // Set the SubRequest in FINISH_FAILED status
  subReq->setStatus(castor::stager::SUBREQUEST_FAILED_FINISHED);

  // Build response
  if (0 != client) {
    // XXX A BasicResponse or a FileResponse could be enough
    // here but the client API would not like it !
    castor::rh::IOResponse res;
    if (0 == subReq->errorCode()) {
      res.setErrorCode(SEINTERNAL);
      std::stringstream ss;
      ss << "Could not retrieve file.\n"
	 << "Please report error and mention file name and the "
	 << "following request ID : " << req->id();
      res.setErrorMessage(ss.str());
    } else {
      res.setErrorCode(subReq->errorCode());
      res.setErrorMessage(subReq->errorMessage());
    }
    res.setStatus(castor::stager::SUBREQUEST_FAILED);
    res.setCastorFileName(subReq->fileName());
    res.setSubreqId(subReq->subreqId());
    res.setReqAssociated(req->reqId());
    // Reply to client
    try {
      castor::replier::RequestReplier *rr = 0;
      try {
        rr = castor::replier::RequestReplier::getInstance();
        rr->sendResponse(client, &res);
      } catch (castor::exception::Exception e) {
        // "Unexpected exception caught"
        castor::dlf::Param params[] =
          {castor::dlf::Param("function", "ErrorSvcThread::process.2"),
           castor::dlf::Param("message", e.getMessage().str()),
           castor::dlf::Param("code", e.code()),
           castor::dlf::Param(suuid)};
        castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_ERRSVC_EXCEPT, 4, params);
        rr = 0;
      }
      // We both update the DB and check whether this was
      // the last subrequest of the request
      if (!stgSvc->updateAndCheckSubRequest(subReq)) {
        if (0 != rr) {
          rr->sendEndResponse(client, req->reqId());
        }
      }
    } catch (castor::exception::Exception e) {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("function", "ErrorSvcThread::process.3"),
         castor::dlf::Param("message", e.getMessage().str()),
         castor::dlf::Param("code", e.code()),
         castor::dlf::Param(suuid)};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_ERRSVC_EXCEPT, 4, params);
    }
  } else {
    // still update the DB to put the subrequest in FAILED_FINISHED
    try {      
      svcs->updateRep(&ad, subReq, true);
    } catch (castor::exception::Exception e) {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("function", "ErrorSvcThread::process.4"),
         castor::dlf::Param("message", e.getMessage().str()),
         castor::dlf::Param("code", e.code()),
         castor::dlf::Param(suuid)};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_ERRSVC_EXCEPT, 4, params);
      serrno = e.code();
    }
  }
  // Cleanup
  if (subReq) delete subReq;
  if (req) delete req;
  if (stgSvc) stgSvc->release();
  return;
}
