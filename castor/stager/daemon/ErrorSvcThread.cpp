/*
 * $Id: ErrorSvcThread.cpp,v 1.7 2005/05/13 10:10:34 sponcec3 Exp $
 */

/*
 * Copyright (C) 2005 by CERN/IT/FIO/DS
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: ErrorSvcThread.cpp,v $ $Revision: 1.7 $ $Date: 2005/05/13 10:10:34 $ CERN IT-FIO/DS Sebastien Ponce";
#endif

/* ================================================================= */
/* Local headers for threads : to be included before ANYTHING else   */
/* ================================================================= */
#include "Cthread_api.h"
#include "Cmutex.h"
#include "stager_uuid.h"                /* Thread-specific uuids */

/* ============== */
/* System headers */
/* ============== */
#include <sstream>

/* ============= */
/* Local headers */
/* ============= */
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
#undef logfunc

#include "stager_service_helper.hpp"
#include "stager_error_service.h"
#include "stager_macros.h"
#include "serrno.h"

#undef NULL
#define NULL 0

/* ---------------------------------------- */
/* stager_error_select()                    */
/*                                          */
/* Purpose: ERROR 'select' part of service  */
/*                                          */
/* Input:  n/a                              */
/*                                          */
/* Output:  (void **) output   Selected     */
/*                                          */
/* Return: 0 [OK] or -1 [ERROR, serrno]     */
/* ---------------------------------------- */

EXTERN_C int DLL_DECL stager_error_select(void **output) {
  char *func =  "stager_error_select";
  int rc;

  STAGER_LOG_ENTER();

  if (output == NULL) {
    serrno = EFAULT;
    return -1;
  }

  castor::stager::SubRequest* subReq = 0;
  castor::Services *svcs;
  castor::stager::IStagerSvc *stgSvc;

  try {

    /* Loading services */
    /* ---------------- */
    STAGER_LOG_VERBOSE(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc =
      svcs->service("OraStagerSvc", castor::SVC_ORASTAGERSVC);
    stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);


    /* Get any new request to do    */
    /* ---------------------------- */
    STAGER_LOG_VERBOSE(NULL,"Getting any request to do");
    subReq = stgSvc->subRequestFailedToDo();

    if (0 == subReq) {
      /* Nothing to do */
      STAGER_LOG_VERBOSE(NULL,"Nothing to do");
      serrno = ENOENT;
      rc = -1;
    } else {
      /* Set uuid for the log */
      /* -------------------- */
      Cuuid_t request_uuid;
      if (string2Cuuid(&request_uuid,(char *) subReq->subreqId().c_str()) == 0) {
        stager_request_uuid = request_uuid;
      }
      std::stringstream msg;
      msg << "Found subRequest " << subReq->id();
      STAGER_LOG_DEBUG(NULL,msg.str().c_str());
      *output = subReq;
      rc = 0;

    }

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    STAGER_LOG_DB_ERROR(NULL,"stager_error_select",
                        e.getMessage().str().c_str());
    rc = -1;
    if (subReq) delete subReq;
  }

  // Cleanup
  stgSvc->release();

  // Return
  STAGER_LOG_RETURN(rc);
}


/* ----------------------------------------- */
/* stager_error_process()                    */
/*                                           */
/* Purpose: Error 'process' part of service  */
/*                                           */
/* Input:  (void *) output    Selection      */
/*                                           */
/* Output: n/a                               */
/*                                           */
/* Return: 0 [OK] or -1 [ERROR, serrno]      */
/* ----------------------------------------- */
EXTERN_C int DLL_DECL stager_error_process(void *output) {

  char *func =  "stager_error_process";
  STAGER_LOG_ENTER();

  serrno = 0;
  if (output == NULL) {
    serrno = EFAULT;
    return -1;
  }

  /* ===========================================================
   *
   * 1) PREPARATION PHASE
   * Retrieving request from the database
   */

  castor::stager::Request* req = 0;
  castor::stager::SubRequest* subReq = 0;
  castor::Services *svcs = 0;
  castor::stager::IStagerSvc *stgSvc = 0;
  castor::IClient *client = 0;

  /* Setting the address to access Oracle */
  /* ------------------------------------ */
  castor::BaseAddress ad;
  ad.setCnvSvcName("OraCnvSvc");
  ad.setCnvSvcType(castor::SVC_ORACNV);

  try {

    /* Loading services */
    /* ---------------- */
    STAGER_LOG_VERBOSE(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc =
      svcs->service("OraStagerSvc", castor::SVC_ORASTAGERSVC);
    stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);

    /* Casting the subRequest */
    /* ---------------------- */
    STAGER_LOG_VERBOSE(NULL, "Casting SubRequest");
    subReq = (castor::stager::SubRequest*)output;
    if (0 == subReq) {
      castor::exception::Internal e;
      e.getMessage() << "Request cast error";
      throw e;
    }

    /* Getting the request */
    /* ------------------- */
    svcs->fillObj(&ad, subReq, castor::OBJ_FileRequest);
    req = subReq->request();
    if (0 == req) {
      castor::exception::Internal e;
      e.getMessage() << "No request associated with subRequest ! Cannot answer !";
      throw e;
    }

    /* Getting the client  */
    /* ------------------- */
    svcs->fillObj(&ad, req, castor::OBJ_IClient);
    client = req->client();
    if (0 == client) {
      castor::exception::Internal e;
      e.getMessage() << "No client associated with request ! Cannot answer !";
      throw e;
    }

  } catch (castor::exception::Exception e) {
    // If we fail here, we do NOT have enough information to
    // reply to the client !
    serrno = e.code();
    STAGER_LOG_DB_ERROR(NULL,"stager_error_process",
                        e.getMessage().str().c_str());
    client = 0;
  }

  /* ===========================================================
   *
   * 2) CALLING PHASE
   * At this point we can send a reply to the client
   */

  // Set the SubRequest in FINISH_FAILED status
  subReq->setStatus(castor::stager::SUBREQUEST_FAILED_FINISHED);

  // Build response
  if (0 != client) {
    // XXX A BasicResponse or a FileResponse could be enough
    // here but the client API would not like it !
    castor::rh::IOResponse res;
    res.setErrorCode(SEINTERNAL);
    std::stringstream ss;
    ss << "Could not retrieve file.\n"
       << "Please report error and mention file name and the "
       << "following request ID : " << req->id();
    res.setErrorMessage(ss.str());
    res.setStatus(castor::stager::SUBREQUEST_FAILED);
    res.setCastorFileName(subReq->fileName());
    res.setSubreqId(subReq->subreqId());
  
    // Reply to client
    try {
      STAGER_LOG_DEBUG(NULL, "Sending Response");
      castor::replier::RequestReplier *rr = 0;
      try {
        rr = castor::replier::RequestReplier::getInstance();
        rr->sendResponse(client, &res);
      } catch (castor::exception::Exception e) {
        serrno = e.code();
        STAGER_LOG_DB_ERROR(NULL, func,
                            e.getMessage().str().c_str());
        rr = 0;
      }
      // We both update the DB and check whether this was
      // the last subrequest of the request
      if (!stgSvc->updateAndCheckSubRequest(subReq)) {
        if (0 != rr) {
          rr->sendEndResponse(client);
        }
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      STAGER_LOG_DB_ERROR(NULL, func,
                          e.getMessage().str().c_str());
    }
  } else {
    // still update the DB to put the subrequest in FAILED_FINISHED
    try {      
      svcs->updateRep(&ad, subReq, true);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      STAGER_LOG_DB_ERROR(NULL, func,
                          e.getMessage().str().c_str());
    }
  }

  // Cleanup
  if (subReq) delete subReq;
  if (req) delete req;
  if (stgSvc) stgSvc->release();
  STAGER_LOG_RETURN(serrno == 0 ? 0 : -1);

}
