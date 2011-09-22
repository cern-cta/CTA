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
