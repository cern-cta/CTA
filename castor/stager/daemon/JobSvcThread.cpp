/*
 * $Id: JobSvcThread.cpp,v 1.4 2004/12/06 16:01:39 jdurand Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: JobSvcThread.cpp,v $ $Revision: 1.4 $ $Date: 2004/12/06 16:01:39 $ CERN IT-ADC/CA Ben Couturier";
#endif

/* ================================================================= */
/* Local headers for threads : to be included before ANYTHING else   */
/* ================================================================= */
#include "Cthread_api.h"
#include "Cmutex.h"

/* ============== */
/* System headers */
/* ============== */
#include <list>
#include <vector>

/* ============= */
/* Local headers */
/* ============= */
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/PutStartRequest.hpp"
#include "castor/stager/UpdateRepRequest.hpp"
#include "castor/stager/MoverCloseRequest.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/rh/GetUpdateStartResponse.hpp"
#include "castor/replier/RequestReplier.hpp"
#undef logfunc

#include "stager_job_service.h"
#include "stager_macros.h"
#include "serrno.h"

#undef NULL
#define NULL 0

/* ------------------------------------- */
/* stager_job_select()                   */
/*                                       */
/* Purpose: Job 'select' part of service */
/*                                       */
/* Input:  n/a                           */
/*                                       */
/* Output:  (void **) output   Selected  */
/*                                       */
/* Return: 0 [OK] or -1 [ERROR, serrno]  */
/* ------------------------------------- */



EXTERN_C int DLL_DECL stager_job_select(void **output) {
  char *func =  "stager_job_select";
  int rc;

  STAGER_LOG_ENTER();

  if (output == NULL) {
    serrno = EFAULT;
    return -1;
  }

  castor::stager::Request* req = 0;
  castor::Services *svcs;
  castor::stager::IStagerSvc *stgSvc;

  try {

    /* Loading services */
    /* ---------------- */
    STAGER_LOG_DEBUG(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc =
      svcs->service("OraStagerSvc", castor::SVC_ORASTAGERSVC);
    stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);


    /* Get any new request to do    */
    /* ---------------------------- */
    STAGER_LOG_DEBUG(NULL,"Getting any request to do");
    std::vector<castor::ObjectsIds> types;
    types.push_back(castor::OBJ_GetUpdateStartRequest);
    castor::stager::Request* req = stgSvc->requestToDo(types);

    if (0 == req) {
      /* Nothing to do */
      STAGER_LOG_DEBUG(NULL,"Nothing to do");
      serrno = ENOENT;
      rc = -1;
    } else {
      STAGER_LOG_DEBUG(NULL,"req FOUND");
      *output = req;
      rc = 0;
    }

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    STAGER_LOG_DB_ERROR(NULL,"stager_job_select",
                        e.getMessage().str().c_str());
    rc = -1;
    if (req) delete req;
  }

  // Cleanup
  stgSvc->release();

  // Return
  STAGER_LOG_RETURN(rc);
}


namespace castor {
  
  namespace stager {

    /**
     * Sends a Response to a client
     * In case of error, on writes a message to the log
     * @param client the client where to send the response
     * @param res the response to send
     */
    void replyToClient(castor::IClient* client,
                       castor::rh::Response* res) {
      char *func =  "castor::stager::replyToClient";
      try {
        STAGER_LOG_DEBUG(NULL, "Sending Response");
        castor::replier::RequestReplier *rr =
          castor::replier::RequestReplier::getInstance();
        rr->sendResponse(client, res);
        rr->sendEndResponse(client);
      } catch (castor::exception::Exception e) {
        serrno = e.code();
        STAGER_LOG_DB_ERROR(NULL, func,
                            e.getMessage().str().c_str());
      }
    }

    /**
     * Handles a StartRequest and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param stgSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_startRequest(castor::stager::Request* req,
                             castor::IClient *client,
                             castor::Services* svcs,
                             castor::stager::IStagerSvc* stgSvc,
                             castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::startRequest";
      castor::stager::FileSystem *fs = 0;
      castor::stager::SubRequest *subreq = 0;
      castor::stager::DiskCopy *dc = 0;
      castor::IClient *cl = 0;
      std::string error;
      std::list<castor::stager::DiskCopyForRecall*> sources;
      castor::stager::StartRequest *sReq;
      
      try {

        /* get the StartRequest */
        /* -------------------- */
        // cannot return 0 since we check the type before calling this method
        sReq = dynamic_cast<castor::stager::StartRequest*> (req);

        /* Loading the subrequest from Oracle */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Loading the subrequest from Oracle");
        ad.setId(sReq->subreqId());
        castor::IObject *obj = svcs->createObj(&ad);
        if (0 == obj) {
          castor::exception::Internal e;
          e.getMessage() << "Found no object for ID:" << sReq->subreqId();
          throw e;
        }
        subreq = dynamic_cast<castor::stager::SubRequest*>(obj);
        if (0 == subreq) {
          castor::exception::Internal e;
          e.getMessage() << "Expected SubRequest found type "
                         << castor::ObjectsIdStrings[obj->type()]
                         << " for ID:" << sReq->subreqId();
          delete obj;
          throw e;
        }

        /* Getting the FileSystem Object      */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Getting the FileSystem Object");
        fs = stgSvc->selectFileSystem(sReq->fileSystem(),
                                      sReq->diskServer());
        if (0==fs) {
          castor::exception::Internal e;
          e.getMessage() << "Could not find suitable filesystem for"
                         << sReq->fileSystem() << "/"
                         << sReq->diskServer();
          throw e;
        }

        /* Invoking the method                */
        /* ---------------------------------- */
        if (castor::OBJ_GetUpdateStartRequest == sReq->type()) {
          STAGER_LOG_DEBUG(NULL, "Invoking getUpdateStart");
          cl = stgSvc->getUpdateStart(subreq, fs, &dc, sources);
        } else {
          STAGER_LOG_DEBUG(NULL, "Invoking PutStart");
          cl = stgSvc->putStart(subreq, fs, &dc);          
        }

        /* Fill DiskCopy with SubRequest           */
        /* --------------------------------------- */
        STAGER_LOG_DEBUG(NULL,"Filling DiskCopy with SubRequest");
        if (0 != dc) {
          svcs->fillObj(&ad, dc, castor::OBJ_SubRequest);
        }

      } catch (castor::exception::Exception e) {
        serrno = e.code();
        error = e.getMessage().str();
        STAGER_LOG_DB_ERROR(NULL, func,
                            e.getMessage().str().c_str());
      }

      /* Build the response             */
      /* ------------------------------ */
      STAGER_LOG_DEBUG(NULL, "Building Response");
      castor::rh::GetUpdateStartResponse res;
      if (0 != serrno) {
        res.setErrorCode(serrno);
        res.setErrorMessage(error);
      } else {
        res.setDiskCopy(dc);
        if (castor::OBJ_GetUpdateStartRequest == sReq->type()) {
          for (std::list<castor::stager::DiskCopyForRecall*>::iterator it =
                 sources.begin();
               it != sources.end();
               it++) {
            res.addSources(*it);
          }
        }
        res.setClient(cl);  
      }
      
      /* Reply To Client                */
      /* ------------------------------ */
      replyToClient(client, &res);
      
      /* Cleanup                        */
      /* ------------------------------ */
      if (fs) delete fs;
      if (subreq) delete subreq;
      if (dc) delete dc;
      if (cl) delete cl;
      if (castor::OBJ_GetUpdateStartRequest == sReq->type()) {
        for (std::list<castor::stager::DiskCopyForRecall*>::iterator it =
               sources.begin();
             it != sources.end();
             it++) {
          delete *it;
        }
      }
    }    
    
    /**
     * Handles a UpdateRepRequest and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param stgSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_updateRepRequest(castor::stager::Request* req,
                                 castor::IClient *client,
                                 castor::Services* svcs,
                                 castor::stager::IStagerSvc* stgSvc,
                                 castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::updateRepRequest";
      std::string error;
      castor::stager::UpdateRepRequest *uReq;
      
      try {

        /* get the UpdateRepRequest */
        /* -------------------- */
        // cannot return 0 since we check the type before calling this method
        uReq = dynamic_cast<castor::stager::UpdateRepRequest*> (req);

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking updateRep");
        stgSvc->updateRep(uReq->address(), uReq->object());

      } catch (castor::exception::Exception e) {
        serrno = e.code();
        error = e.getMessage().str();
        STAGER_LOG_DB_ERROR(NULL, func,
                            e.getMessage().str().c_str());
      }

      /* Build the response             */
      /* ------------------------------ */
      STAGER_LOG_DEBUG(NULL, "Building Response");
      castor::rh::BasicResponse res;
      if (0 != serrno) {
        res.setErrorCode(serrno);
        res.setErrorMessage(error);
      }
      
      /* Reply To Client                */
      /* ------------------------------ */
      replyToClient(client, &res);
      
      /* Cleanup                        */
      /* ------------------------------ */
      if (uReq->address()) delete uReq->address();
      if (uReq->object()) delete uReq->object();
    }    
    
    /**
     * Handles a MoverCloseRequest and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param stgSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_moverCloseRequest(castor::stager::Request* req,
                                 castor::IClient *client,
                                 castor::Services* svcs,
                                 castor::stager::IStagerSvc* stgSvc,
                                 castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::moverCloseRequest";
      castor::stager::SubRequest *subreq = 0;
      std::string error;
      castor::stager::MoverCloseRequest *mcReq;

      try {

        /* get the MoverCloseRequest */
        /* -------------------- */
        // cannot return 0 since we check the type before calling this method
        mcReq = dynamic_cast<castor::stager::MoverCloseRequest*> (req);

        /* Loading the subrequest from Oracle */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Loading the subrequest from Oracle");
        ad.setId(mcReq->subReqId());
        castor::IObject *obj = svcs->createObj(&ad);
        if (0 == obj) {
          castor::exception::Internal e;
          e.getMessage() << "Found no object for ID:" << mcReq->subReqId();
          throw e;
        }
        subreq = dynamic_cast<castor::stager::SubRequest*>(obj);
        if (0 == subreq) {
          castor::exception::Internal e;
          e.getMessage() << "Expected SubRequest found type "
                         << castor::ObjectsIdStrings[obj->type()]
                         << " for ID:" << mcReq->subReqId();
          delete obj;
          throw e;
        }

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking prepareForMigration");
        stgSvc->prepareForMigration(subreq, mcReq->fileSize());

      } catch (castor::exception::Exception e) {
        serrno = e.code();
        error = e.getMessage().str();
        STAGER_LOG_DB_ERROR(NULL, func,
                            e.getMessage().str().c_str());
      }

      /* Build the response             */
      /* ------------------------------ */
      STAGER_LOG_DEBUG(NULL, "Building Response");
      castor::rh::BasicResponse res;
      if (0 != serrno) {
        res.setErrorCode(serrno);
        res.setErrorMessage(error);
      }
      
      /* Reply To Client                */
      /* ------------------------------ */
      replyToClient(client, &res);
      
    }    
    
  } // End of namespace stager
  
} // End of namespace castor


/* -------------------------------------- */
/* stager_job_process()                   */
/*                                        */
/* Purpose: Job 'process' part of service */
/*                                        */
/* Input:  (void *) output    Selection   */
/*                                        */
/* Output: n/a                            */
/*                                        */
/* Return: 0 [OK] or -1 [ERROR, serrno]   */
/* -------------------------------------- */
EXTERN_C int DLL_DECL stager_job_process(void *output) {

  char *func =  "stager_job_process";
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
    STAGER_LOG_DEBUG(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc =
      svcs->service("OraStagerSvc", castor::SVC_ORASTAGERSVC);
    stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);

    /* Casting the request */
    /* ------------------- */
    STAGER_LOG_DEBUG(NULL, "Casting Request");
    req = (castor::stager::Request*)output;
    if (0 == req) {
      castor::exception::Internal e;
      e.getMessage() << "Request cast error";
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
    STAGER_LOG_DB_ERROR(NULL,"stager_job_select",
                        e.getMessage().str().c_str());
    if (req) delete req;
    if (stgSvc) stgSvc->release();
    return -1;
  }

  /* ===========================================================
   *
   * 2) CALLING PHASE
   * At this point we can send a reply to the client
   * We get prepared to call the method
   */

  switch (req->type()) {
    
  case castor::OBJ_GetUpdateStartRequest:
  case castor::OBJ_PutStartRequest:
    castor::stager::handle_startRequest
      (req, client, svcs, stgSvc, ad);
    break;

  case castor::OBJ_UpdateRepRequest:
    castor::stager::handle_updateRepRequest
      (req, client, svcs, stgSvc, ad);
    break;

  case castor::OBJ_MoverCloseRequest:
    castor::stager::handle_moverCloseRequest
      (req, client, svcs, stgSvc, ad);
    break;

  default:
    castor::exception::Internal e;
    e.getMessage() << "Unknown Request type : "
                   << castor::ObjectsIdStrings[req->type()];
    throw e;

  }  

  // Cleanup
  if (req) delete req;
  if (stgSvc) stgSvc->release();
  STAGER_LOG_RETURN(serrno == 0 ? 0 : -1);
}
