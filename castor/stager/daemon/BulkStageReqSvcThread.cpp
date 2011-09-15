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
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/stager/BulkRequestResult.hpp"
#include "castor/replier/RequestReplier.hpp"
#include <vector>
#include "castor/stager/FileResult.hpp"
#include "castor/Constants.hpp"
#include "castor/rh/FileResponse.hpp"

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

  // Useful variables
  Cuuid_t uuid = nullCuuid;

  // Retrieving request information
  // Note that casting the request will never give 0
  // since select does return a request for sure
  castor::stager::BulkRequestResult *result =
    dynamic_cast<castor::stager::BulkRequestResult*>(param);

  try {

    // get request replier
    castor::replier::RequestReplier *rr =
      castor::replier::RequestReplier::getInstance();
    
    // Log and prepare client response
    for (std::vector<castor::stager::FileResult>::const_iterator it =
           result->subResults().begin();
         it != result->subResults().end();
         it++) {
      switch (result->reqType()) {
      case castor::OBJ_StageAbortRequest:
        // "Abort processed"
        castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM,
                                castor::stager::daemon::STAGER_BLKSTGSVC_ABORT,
                                it->fileId(), it->nsHost());
        break;
      default:
        // "Unknown request processed"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Type", result->reqType())};
        castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, 
                                castor::stager::daemon::STAGER_BLKSTGSVC_UNKREQ,
                                it->fileId(), it->nsHost(), 1, params);
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
    for (std::vector<castor::stager::FileResult>::const_iterator it =
           result->subResults().begin();
         it != result->subResults().end();
         it++) {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "BulkStageReqSvcThread::process"),
         castor::dlf::Param("Message", e.getMessage().str()),
         castor::dlf::Param("Code", e.code())};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT,
                              it->fileId(), it->nsHost(), 3, params);
    }
  }

  // Final cleanup
  delete result;
}
