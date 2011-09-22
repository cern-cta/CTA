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
 * @(#)$RCSfile: BulkStageReqSvcThread.cpp,v $ $Revision: 1.67 $ $Release$ $Date: 2009/08/18 09:42:55 $ $Author: waldron $
 *
 * Service thread for bulk requests
 *
 * @author castor dev team
 *****************************************************************************/

#include "castor/stager/daemon/BulkStageReqSvcThread.hpp"
#include "castor/IObject.hpp"
#include "castor/System.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/PermissionDenied.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/stager/BulkRequestResult.hpp"
#include "castor/stager/StageRepackRequest.hpp"
#include "castor/replier/RequestReplier.hpp"
#include <vector>
#include "castor/stager/FileResult.hpp"
#include "castor/Constants.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/rh/FileResponse.hpp"
#include "Cupv_api.h"
#include <vmgr_api.h>
#include <errno.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::daemon::BulkStageReqSvcThread::BulkStageReqSvcThread() throw () :
  castor::server::SelectProcessThread() {}

//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::stager::daemon::BulkStageReqSvcThread::select() throw() {
  try {
    castor::IService* svc =
      castor::BaseObject::services()->service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
    castor::IObject* req = 0;
    castor::stager::IStagerSvc* stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);
    // as dlopen is not reentrant (i.e., symbols might be still loading now due to the dlopen
    // of another thread), it may happen that the service is not yet valid or dynamic_cast fails.
    // In such a case we simply give up for this round.
    if(0 == stgSvc) return 0;
    req = stgSvc->processBulkRequest("BulkStageReqSvc");
    return req;
  } catch (castor::exception::Exception& e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "BulkStageReqSvcThread::select"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, castor::stager::daemon::STAGER_SERVICES_EXCEPTION, 3, params);
    return 0;
  }
}

//-----------------------------------------------------------------------------
// handleRepackFirstStep (private)
//-----------------------------------------------------------------------------
castor::stager::BulkRequestResult*
castor::stager::daemon::BulkStageReqSvcThread::handleRepackFirstStep
(castor::stager::StageRepackRequest *req)
  throw(castor::exception::Exception) {
  // Check if the user has ADMIN privileges so that they can perform repack requests
  const castor::rh::Client *client = dynamic_cast<const castor::rh::Client*>(req->client());
  std::string srcHostName = castor::System::ipAddressToHostname(client->ipAddress());
  int rc = Cupv_check(req->euid(), req->egid(), srcHostName.c_str(), "", P_ADMIN);
  if ((rc < 0) && (serrno != EACCES)) {
    castor::exception::Exception e(serrno);
    e.getMessage() << "Failed Cupv_check call for VID " << req->repackVid() << ", "
                   << req->euid() << ":" << req->egid() << " (ADMIN)";
    throw e;
  } else if (rc < 0) {
    castor::exception::PermissionDenied e;
    e.getMessage() << "Not authorized to perform repack requests";
    throw e;
  }
  // check vmgr to see whether the tape is available
  struct vmgr_tape_info_byte_u64 vmgrTapeInfo;
  // Note that the side is HARDCODED to 0, as this field is now deprecated
  if (-1 == vmgr_querytape_byte_u64(req->repackVid().c_str(), 0, &vmgrTapeInfo, 0)) {
    castor::exception::Exception e(serrno);
    e.getMessage() << "Failed vmgr_querytape check for VID " << req->repackVid() << ", "
                   << req->euid() << ":" << req->egid();
    throw e;
  } else {
    // when STANDBY tapes will be supported, they will
    // be let through, while we consider any of the following as permanent errors
    if(((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
       ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ||
       ((vmgrTapeInfo.status & EXPORTED) == EXPORTED)) {
      // "Tape disabled, could not be used for recall"
      castor::exception::InvalidArgument e;
      e.getMessage() << "Tape disabled, could not be used for recall : " << req->repackVid();
      throw e;
    }
  }
  // handles the repack request directly at the DB level and returns result
  castor::IService* svc = castor::BaseObject::services()->service("DbStageSvc", castor::SVC_DBSTAGERSVC);
  castor::stager::IStagerSvc* stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);
  return stgSvc->handleRepackRequest(req->id());
}

//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::daemon::BulkStageReqSvcThread::process
(castor::IObject *param) throw() {
  // the final result that will be used to answer the client
  castor::stager::BulkRequestResult *result = 0;
  try {
    // get a pointer to the request replier
    castor::replier::RequestReplier *rr = castor::replier::RequestReplier::getInstance();
    // let's find out the type of object we got
    switch (param->type()) {
    case castor::OBJ_StageRepackRequest:
      {
        castor::stager::StageRepackRequest *req =
          dynamic_cast<castor::stager::StageRepackRequest*>(param);
        try {
          // RepackRequest. We will preprocess it (check Cupv right and VMGR status)
          // before we go back to the stager DB to get the final result
          // "Repack initiated" message
          castor::dlf::Param params[] = {castor::dlf::Param("TPVID", req->repackVid()),
                                         castor::dlf::Param("REQID", req->reqId())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                                  castor::stager::daemon::STAGER_BLKSTGSVC_REPACK,
                                  0, "", 2, params);
          result = handleRepackFirstStep(req);
          delete req;
          break;
        } catch (castor::exception::Exception e) {
          // "Unexpected exception caught"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Function", "BulkStageReqSvcThread::process"),
             castor::dlf::Param("Message", e.getMessage().str()),
             castor::dlf::Param("Code", e.code()),
             castor::dlf::Param("REQID", req->reqId())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT,
                                  0, "", 4, params);
          // reply to client
          castor::rh::BasicResponse res;
          res.setReqAssociated(req->reqId());
          res.setErrorCode(e.code());
          res.setErrorMessage(e.getMessage().str());
          rr->sendResponse(req->client(), &res, true);
          // cleanup and return
          delete req;
          return;
        }
      }
    case castor::OBJ_BulkRequestResult:
      {
        // We got directly a result (the request did not need preprocessing)
        result = dynamic_cast<castor::stager::BulkRequestResult*>(param);
        break;
      }
    }
    // Log and prepare client response
    for (std::vector<castor::stager::FileResult>::const_iterator it =
           result->subResults().begin();
         it != result->subResults().end();
         it++) {
      switch (result->reqType()) {
      case castor::OBJ_StageAbortRequest:
        // "Abort processed" message
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                                castor::stager::daemon::STAGER_BLKSTGSVC_ABORT,
                                it->fileId(), it->nsHost());
        break;
      case castor::OBJ_StageRepackRequest:
        {
          // "Repack initiated" message
          castor::dlf::Param params[] =
            {castor::dlf::Param("REQID", result->reqId())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                                  castor::stager::daemon::STAGER_BLKSTGSVC_REPACK,
                                  it->fileId(), it->nsHost(), 1, params);
        }
        break;
      default:
        {
          // "Unknown request processed"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Type", result->reqType()),
             castor::dlf::Param("REQID", result->reqId())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 
                                  castor::stager::daemon::STAGER_BLKSTGSVC_UNKREQ,
                                  it->fileId(), it->nsHost(), 2, params);
        }
      }
      // reply to client
      castor::rh::FileResponse res;
      res.setReqAssociated(result->reqId());
      res.setFileId(it->fileId());
      if (it->errorCode() != 0) {
        res.setErrorCode(it->errorCode());
        res.setErrorMessage(it->errorMessage());
      }
      rr->sendResponse(&result->client(), &res, false);
    }
    rr->sendEndResponse(&result->client(), result->reqId());
  } catch (castor::exception::Exception& e) {
    if (0 != result) {
      for (std::vector<castor::stager::FileResult>::const_iterator it =
             result->subResults().begin();
           it != result->subResults().end();
           it++) {
        // "Unexpected exception caught"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Function", "BulkStageReqSvcThread::process"),
           castor::dlf::Param("Message", e.getMessage().str()),
           castor::dlf::Param("Code", e.code())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT,
                                it->fileId(), it->nsHost(), 3, params);
      }
    } else {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "BulkStageReqSvcThread::process")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT,
                              0, "", 1, params);
    }
  }
  // Final cleanup
  delete result;
}
