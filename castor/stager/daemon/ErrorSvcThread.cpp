/******************************************************************************
 *                castor/stager/daemon/ErrorSvcThread.cpp
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
 * @(#)$RCSfile: ErrorSvcThread.cpp,v $ $Revision: 1.23 $ $Release$ $Date: 2009/05/18 16:47:48 $ $Author: itglp $
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
#include "castor/stager/ErrorFileRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/stager/daemon/ErrorSvcThread.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::daemon::ErrorSvcThread::ErrorSvcThread() throw () {}

//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::stager::daemon::ErrorSvcThread::select()
  throw() {
  try {
    // get the Svc. Note that we cannot cache it since we
    // would not be thread safe
    castor::Services *svcs = castor::BaseObject::services();
    castor::IService *svc = svcs->service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
    castor::stager::IStagerSvc *stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);
    // as dlopen is not reentrant (i.e., symbols might be still loading now due to the dlopen
    // of another thread), it may happen that the service is not yet valid or dynamic_cast fails.
    // In such a case we simply give up for this round.
    if(0 == stgSvc) return 0;
    castor::stager::SubRequest* subReq = stgSvc->subRequestFailedToDo();
    return subReq;
  } catch (castor::exception::Exception& e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "ErrorSvcThread::select"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_ERRSVC_EXCEPT, 3, params);
    return 0;
  }
}

//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::daemon::ErrorSvcThread::process
(castor::IObject *param) throw() {
  // Useful variables
  castor::stager::ErrorFileRequest* req = 0;
  castor::stager::SubRequest* subReq = 0;
  Cuuid_t suuid = nullCuuid;
  Cuuid_t uuid = nullCuuid;
  castor::IClient *client = 0;

  // get the Svc. Note that we cannot cache it since we
  // would not be thread safe
  castor::Services *svcs = castor::BaseObject::services();
  castor::IService *svc = svcs->service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
  castor::stager::IStagerSvc *stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);
  // Extracting data; actual fetches from the db
  // have been performed by subReqFailedToDo.
  // Note that casting the subrequest will
  // never be null since select returns one for sure
  subReq = dynamic_cast<castor::stager::SubRequest*>(param);
  string2Cuuid(&suuid, (char*)subReq->subreqId().c_str());
  req = dynamic_cast<castor::stager::ErrorFileRequest*>(subReq->request());
  string2Cuuid(&uuid, (char*)req->reqId().c_str());
  client = req->client();

  // Build response
  // XXX A BasicResponse or a FileResponse could be enough
  // here but the client API would not like it !
  castor::rh::IOResponse res;
  if (0 == subReq->errorCode()) {
    res.setErrorCode(SEINTERNAL);
    std::stringstream ss;
    ss << "Internal error, request ID was " << req->id();
    res.setErrorMessage(ss.str());
  } else {
    res.setErrorCode(subReq->errorCode());
    if (subReq->errorMessage().empty()) {
      res.setErrorMessage(sstrerror(subReq->errorCode()));
    } else {
      res.setErrorMessage(subReq->errorMessage());
    }
  }
  res.setStatus(castor::stager::SUBREQUEST_FAILED);
  // We always take the filename from the original request as it might
  // not be normalized. Moreover, there are cases (e.g. failures of stageRm
  // or putDone) whereby the castorFile link is not updated (in which case
  // we don't return the fileid).
  res.setCastorFileName(subReq->fileName());
  if(subReq->castorFile()) {
    res.setFileId(subReq->castorFile()->fileId());
  }
  res.setSubreqId(subReq->subreqId());
  res.setReqAssociated(req->reqId());
  // Reply to client
  castor::replier::RequestReplier *rr = 0;
  try {
    rr = castor::replier::RequestReplier::getInstance();
    rr->sendResponse(client, &res);
    castor::dlf::Param params[] =
      {castor::dlf::Param("ErrorMessage", res.errorMessage()),
       castor::dlf::Param(suuid)};
    if(subReq->castorFile()) {
      struct Cns_fileid nsid;
      nsid.fileid = subReq->castorFile()->fileId();
      strncpy(nsid.server, subReq->castorFile()->nsHost().c_str(), sizeof(nsid.server));
      castor::dlf::dlf_writep(uuid, DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, 2, params, &nsid);
    } else {
      castor::dlf::dlf_writep(uuid, DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, 2, params);
    }
  } catch (castor::exception::Exception& e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "ErrorSvcThread::process.1"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code()),
       castor::dlf::Param("OriginalErrorMessage", res.errorMessage()),
       castor::dlf::Param(suuid)};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_ERRSVC_EXCEPT, 5, params);
    rr = 0;
  }
  try {
    // We both update the DB and check whether this was
    // the last subrequest of the request
    if (!stgSvc->updateAndCheckSubRequest(subReq)) {
      if (0 != rr) {
        rr->sendEndResponse(client, req->reqId());
      }
    }
  } catch (castor::exception::Exception& e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "ErrorSvcThread::process.2"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code()),
       castor::dlf::Param(suuid)};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_ERRSVC_EXCEPT, 4, params);
  }
  // Cleanup
  if(subReq->castorFile()) delete subReq->castorFile();
  delete req;  // drops the subReq and the client as well
  return;
}
