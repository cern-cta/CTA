/*
 * $Id: JobSvcThread.cpp,v 1.32 2005/07/21 09:13:11 itglp Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: JobSvcThread.cpp,v $ $Revision: 1.32 $ $Date: 2005/07/21 09:13:11 $ CERN IT-ADC/CA Ben Couturier";
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
#include <list>
#include <vector>
#include <sstream>

/* ============= */
/* Local headers */
/* ============= */
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/GetUpdateDone.hpp"
#include "castor/stager/GetUpdateFailed.hpp"
#include "castor/stager/PutFailed.hpp"
#include "castor/stager/PutStartRequest.hpp"
#include "castor/stager/PutDoneStart.hpp"
#include "castor/stager/Disk2DiskCopyDoneRequest.hpp"
#include "castor/stager/MoverCloseRequest.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/rh/GetUpdateStartResponse.hpp"
#undef logfunc

#include "stager_service_helper.hpp"
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
  castor::stager::IJobSvc *jobSvc;

  try {

    /* Loading services */
    /* ---------------- */
    STAGER_LOG_VERBOSE(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc =
      svcs->service("DbJobSvc", castor::SVC_DBJOBSVC);
    jobSvc = dynamic_cast<castor::stager::IJobSvc*>(svc);


    /* Get any new request to do    */
    /* ---------------------------- */
    STAGER_LOG_VERBOSE(NULL,"Getting any request to do");
    std::vector<castor::ObjectsIds> types;
    types.push_back(castor::OBJ_GetUpdateStartRequest);
    types.push_back(castor::OBJ_PutStartRequest);
    types.push_back(castor::OBJ_PutDoneStart);
    types.push_back(castor::OBJ_MoverCloseRequest);
    types.push_back(castor::OBJ_Disk2DiskCopyDoneRequest);
    types.push_back(castor::OBJ_GetUpdateDone);
    types.push_back(castor::OBJ_GetUpdateFailed);
    types.push_back(castor::OBJ_PutFailed);
    castor::stager::Request* req = jobSvc->requestToDo(types);

    if (0 == req) {
      /* Nothing to do */
      STAGER_LOG_VERBOSE(NULL,"Nothing to do");
      serrno = ENOENT;
      rc = -1;
    } else {
      /* Set uuid for the log */
      /* -------------------- */
      Cuuid_t request_uuid;
      if (string2Cuuid(&request_uuid,(char *) req->reqId().c_str()) == 0) {
        stager_request_uuid = request_uuid;
      }
      std::stringstream msg;
      msg << "Found request " << req->id();
      STAGER_LOG_DEBUG(NULL,msg.str().c_str());
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
  jobSvc->release();

  // Return
  STAGER_LOG_RETURN(rc);
}


namespace castor {

  namespace stager {

    /**
     * Handles a StartRequest and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param jobSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_startRequest(castor::stager::Request* req,
                             castor::IClient *client,
                             castor::Services* svcs,
                             castor::stager::IJobSvc* jobSvc,
                             castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::startRequest";
      castor::stager::FileSystem *fs = 0;
      castor::stager::SubRequest *subreq = 0;
      castor::stager::DiskCopy *dc = 0;
      std::string error;
      std::list<castor::stager::DiskCopyForRecall*> sources;
      castor::stager::StartRequest *sReq;
      bool emptyFile;

      try {

        /* get the StartRequest */
        /* -------------------- */
        // cannot return 0 since we check the type before calling this method
        sReq = dynamic_cast<castor::stager::StartRequest*> (req);

        /* Loading the subrequest from db */
        /* ------------------------------ */
        STAGER_LOG_VERBOSE(NULL, "Loading the subrequest from db");
        ad.setTarget(sReq->subreqId());
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

        /* Set uuid for the log */
        /* -------------------- */
        Cuuid_t subrequest_uuid;
        if (string2Cuuid(&subrequest_uuid,
                         (char *) subreq->subreqId().c_str()) == 0) {
          stager_subrequest_uuid = subrequest_uuid;
        }

        /* Getting the FileSystem Object      */
        /* ---------------------------------- */
        STAGER_LOG_VERBOSE(NULL, "Getting the FileSystem Object");
        fs = jobSvc->selectFileSystem(sReq->fileSystem(),
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
          dc = jobSvc->getUpdateStart(subreq, fs, sources, &emptyFile);
        } else {
          STAGER_LOG_DEBUG(NULL, "Invoking PutStart");
          dc = jobSvc->putStart(subreq, fs);
        }

        /* Fill DiskCopy with SubRequest           */
        /* --------------------------------------- */
        STAGER_LOG_VERBOSE(NULL,"Filling DiskCopy with SubRequest");
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
        res.setEmptyFile(emptyFile);
      }

      /* Reply To Client                */
      /* ------------------------------ */
      replyToClient(client, &res);

      /* Cleanup                        */
      /* ------------------------------ */
      if (0 != fs) {
        castor::stager::DiskServer *ds = fs->diskserver();
        if (0 != ds) {
          delete ds;
        }
        delete fs;
      }
      if (subreq) delete subreq;
      if (dc) {
        std::vector<castor::stager::SubRequest*> srs = dc->subRequests();
        for (std::vector<castor::stager::SubRequest*>::iterator it =
               srs.begin();
             it != srs.end();
             it++) {
          delete *it;
        }
        delete dc;
      }
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
     * Handles a putDoneStartRequest and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param jobSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_putDoneStartRequest(castor::stager::Request* req,
				    castor::IClient *client,
				    castor::Services* svcs,
				    castor::stager::IJobSvc* jobSvc,
				    castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::startRequest";
      castor::stager::DiskCopy *dc = 0;
      std::string error;
      castor::stager::PutDoneStart *sReq;

      try {

        /* get the StartRequest */
        /* -------------------- */
        // cannot return 0 since we check the type before calling this method
        sReq = dynamic_cast<castor::stager::PutDoneStart*> (req);

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking putDoneStart");
        dc = jobSvc->putDoneStart(sReq->subreqId());

        /* Fill DiskCopy with SubRequest           */
        /* --------------------------------------- */
        STAGER_LOG_VERBOSE(NULL,"Filling DiskCopy with SubRequest");
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
      castor::rh::StartResponse res;
      if (0 != serrno) {
        res.setErrorCode(serrno);
        res.setErrorMessage(error);
      } else {
        res.setDiskCopy(dc);
      }

      /* Reply To Client                */
      /* ------------------------------ */
      replyToClient(client, &res);

      /* Cleanup                        */
      /* ------------------------------ */
      if (dc) {
        std::vector<castor::stager::SubRequest*> srs = dc->subRequests();
        for (std::vector<castor::stager::SubRequest*>::iterator it =
               srs.begin();
             it != srs.end();
             it++) {
          delete *it;
        }
        delete dc;
      }
    }

    /**
     * Handles a Disk2DiskCopyDoneRequest and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param jobSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_disk2DiskCopyDoneRequest(castor::stager::Request* req,
                                         castor::IClient *client,
                                         castor::Services* svcs,
                                         castor::stager::IJobSvc* jobSvc,
                                         castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::disk2DiskCopyDoneRequest";
      std::string error;
      castor::stager::Disk2DiskCopyDoneRequest *uReq;

      try {

        /* get the Disk2DiskCopyDoneRequest */
        /* -------------------------------- */
        // cannot return 0 since we check the type before calling this method
        uReq = dynamic_cast<castor::stager::Disk2DiskCopyDoneRequest*> (req);

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking disk2DiskCopyDone");
        jobSvc->disk2DiskCopyDone(uReq->diskCopyId(), 
                                  (castor::stager::DiskCopyStatusCodes)uReq->status());

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

    /**
     * Handles a MoverCloseRequest and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param jobSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_moverCloseRequest(castor::stager::Request* req,
                                  castor::IClient *client,
                                  castor::Services* svcs,
                                  castor::stager::IJobSvc* jobSvc,
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

        /* Loading the subrequest from db */
        /* -------------------------------*/
        STAGER_LOG_VERBOSE(NULL, "Loading the subrequest from db");
        ad.setTarget(mcReq->subReqId());
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

        /* Set uuid for the log */
        /* -------------------- */
        Cuuid_t subrequest_uuid;
        if (string2Cuuid(&subrequest_uuid,
                         (char *) subreq->subreqId().c_str()) == 0) {
          stager_subrequest_uuid = subrequest_uuid;
        }

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking prepareForMigration");
        jobSvc->prepareForMigration(subreq, mcReq->fileSize());

        // Cleaning
        delete obj;

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

    /**
     * Handles a GetUpdateDone request and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param jobSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_getUpdateDoneRequest(castor::stager::Request* req,
                                     castor::IClient *client,
                                     castor::Services* svcs,
                                     castor::stager::IJobSvc* jobSvc,
                                     castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::getUpdateDone";
      std::string error;
      castor::stager::GetUpdateDone *uReq;

      try {

        /* get the GetUpdateDoneRequest */
        /* ---------------------------- */
        // cannot return 0 since we check the type before calling this method
        uReq = dynamic_cast<castor::stager::GetUpdateDone*> (req);

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking getUpdateDone");
        jobSvc->getUpdateDone(uReq->subReqId());

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

    /**
     * Handles a GetUpdateFailed request and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param jobSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_getUpdateFailedRequest(castor::stager::Request* req,
                                       castor::IClient *client,
                                       castor::Services* svcs,
                                       castor::stager::IJobSvc* jobSvc,
                                       castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::getUpdateFailed";
      std::string error;
      castor::stager::GetUpdateFailed *uReq;

      try {

        /* get the GetUpdateFailedRequest */
        /* ------------------------------ */
        // cannot return 0 since we check the type before calling this method
        uReq = dynamic_cast<castor::stager::GetUpdateFailed*> (req);

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking getUpdateFailed");
        jobSvc->getUpdateFailed(uReq->subReqId());

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

    /**
     * Handles a PutFailed request and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param jobSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_putFailedRequest(castor::stager::Request* req,
                                 castor::IClient *client,
                                 castor::Services* svcs,
                                 castor::stager::IJobSvc* jobSvc,
                                 castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::PutFailed";
      std::string error;
      castor::stager::PutFailed *uReq;

      try {

        /* get the GetUpdateDoneRequest */
        /* ---------------------------- */
        // cannot return 0 since we check the type before calling this method
        uReq = dynamic_cast<castor::stager::PutFailed*> (req);

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking putFailed");
        jobSvc->putFailed(uReq->subReqId());

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
  castor::stager::IJobSvc *jobSvc = 0;
  castor::IClient *client = 0;

  /* Setting the address to access db */
  /* -------------------------------- */
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);

  try {

    /* Loading services */
    /* ---------------- */
    STAGER_LOG_VERBOSE(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc =
      svcs->service("DbJobSvc", castor::SVC_DBJOBSVC);
    jobSvc = dynamic_cast<castor::stager::IJobSvc*>(svc);

    /* Casting the request */
    /* ------------------- */
    STAGER_LOG_VERBOSE(NULL, "Casting Request");
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
    if (jobSvc) jobSvc->release();
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
      (req, client, svcs, jobSvc, ad);
    break;

  case castor::OBJ_Disk2DiskCopyDoneRequest:
    castor::stager::handle_disk2DiskCopyDoneRequest
      (req, client, svcs, jobSvc, ad);
    break;

  case castor::OBJ_MoverCloseRequest:
    castor::stager::handle_moverCloseRequest
      (req, client, svcs, jobSvc, ad);
    break;

  case castor::OBJ_GetUpdateDone:
    castor::stager::handle_getUpdateDoneRequest
      (req, client, svcs, jobSvc, ad);
    break;

  case castor::OBJ_GetUpdateFailed:
    castor::stager::handle_getUpdateFailedRequest
      (req, client, svcs, jobSvc, ad);
    break;

  case castor::OBJ_PutFailed:
    castor::stager::handle_putFailedRequest
      (req, client, svcs, jobSvc, ad);
    break;

  case castor::OBJ_PutDoneStart:
    castor::stager::handle_putDoneStartRequest
      (req, client, svcs, jobSvc, ad);
    break;

  default:
    // Issue error message
    serrno = EINVAL;
    std::ostringstream oss;
    oss << "Unknown Request type : "
        << castor::ObjectsIdStrings[req->type()];
    STAGER_LOG_DB_ERROR(NULL,"stager_job_select",
                        oss.str().c_str());
    // Inform client
    STAGER_LOG_DEBUG(NULL, "Building Response");
    castor::rh::BasicResponse res;
    res.setErrorCode(serrno);
    res.setErrorMessage(oss.str().c_str());
    castor::stager::replyToClient(client, &res);

  }

  // Delete Request From the database
  svcs->deleteRep(&ad, req, true);

  // Cleanup
  if (req) delete req;
  if (jobSvc) jobSvc->release();
  STAGER_LOG_RETURN(serrno == 0 ? 0 : -1);
}
