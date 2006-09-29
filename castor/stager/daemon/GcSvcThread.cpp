/*
 * $Id: GcSvcThread.cpp,v 1.12 2006/09/29 07:18:41 gtaur Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: GcSvcThread.cpp,v $ $Revision: 1.12 $ $Date: 2006/09/29 07:18:41 $ CERN IT-ADC/CA Ben Couturier";
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
#include <vector>

/* ============= */
/* Local headers */
/* ============= */
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IGCSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/GCFileList.hpp"
#include "castor/stager/GCFile.hpp"
#include "castor/stager/Files2Delete.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "castor/rh/GCFilesResponse.hpp"
#include "castor/rh/BasicResponse.hpp"
#undef logfunc

#include "stager_service_helper.hpp"
#include "stager_gc_service.h"
#include "stager_macros.h"
#include "serrno.h"

#undef NULL
#define NULL 0

/* ------------------------------------- */
/* stager_gc_select()                    */
/*                                       */
/* Purpose: GC 'select' part of service  */
/*                                       */
/* Input:  n/a                           */
/*                                       */
/* Output:  (void **) output   Selected  */
/*                                       */
/* Return: 0 [OK] or -1 [ERROR, serrno]  */
/* ------------------------------------- */



EXTERN_C int DLL_DECL stager_gc_select(void **output) {
  char *func =  "stager_gc_select";
  int rc;

  STAGER_LOG_ENTER();

  if (output == NULL) {
    serrno = EFAULT;
    return -1;
  }

  castor::stager::Request* req = 0;
  castor::Services *svcs;
  castor::stager::IGCSvc *gcSvc;

  try {

    /* Loading services */
    /* ---------------- */
    STAGER_LOG_VERBOSE(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc =
      svcs->service("DbGCSvc", castor::SVC_DBGCSVC);
    gcSvc = dynamic_cast<castor::stager::IGCSvc*>(svc);

     if(!gcSvc) {
      STAGER_LOG_DB_ERROR(NULL,"stager_gc_service","Impossible create/locate castor::SVC_DBGCSVC");
      return -1;
    }


    /* Get any new request to do    */
    /* ---------------------------- */
    STAGER_LOG_VERBOSE(NULL,"Getting any request to do");
    castor::stager::Request* req = gcSvc->requestToDo();

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
      STAGER_LOG_USAGE(NULL,msg.str().c_str());
      *output = req;
      rc = 0;

    }

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    STAGER_LOG_DB_ERROR(NULL,"stager_gc_select",
                        e.getMessage().str().c_str());
    rc = -1;
    if (req) delete req;
  }

  // Cleanup
  gcSvc->release();

  // Return
  STAGER_LOG_RETURN(rc);
}


namespace castor {

  namespace stager {

    /**
     * Handles FilesDeleted and FilesDeletionFailed requests
     * and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param gcSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_filesDeletedOrFailed
    (castor::stager::Request* req,
     castor::IClient *client,
     castor::Services* svcs,
     castor::stager::IGCSvc* gcSvc,
     castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::filesDeletedOrFailed";
      std::string error;

      try {

        /* get the GCFileList */
        /* ---------------- */
        // cannot return 0 since we check the type before calling this method
        castor::stager::GCFileList *uReq =
          dynamic_cast<castor::stager::GCFileList*> (req);

        // Fills it with files to be deleted
        svcs->fillObj(&ad, req, castor::OBJ_GCFile);

        /* Invoking the method                */
        /* ---------------------------------- */
        std::vector<u_signed64*> arg;
        u_signed64* argArray = new u_signed64[uReq->files().size()];
        int i = 0;
        for (std::vector<castor::stager::GCFile*>::iterator it =
               uReq->files().begin();
             it != uReq->files().end();
             it++) {
          argArray[i] = (*it)->diskCopyId();
          arg.push_back(&(argArray[i]));
          i++;
        }
        if (castor::OBJ_FilesDeleted == req->type()) {
          STAGER_LOG_USAGE(NULL, "Invoking filesDeleted");
          gcSvc->filesDeleted(arg);
        } else {
          STAGER_LOG_USAGE(NULL, "Invoking filesDeletionFailed");
          gcSvc->filesDeletionFailed(arg);
        }
        delete[] argArray;

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
      replyToClient(client, &res, req->reqId());

    }

    /**
     * Handles a Files2Delete request and replies to client.
     * @param req the request to handle
     * @param client the client where to send the response
     * @param svcs the Services object to use
     * @param gcSvc the stager service to use
     * @param ad the address where to load/store objects in the DB
     */
    void handle_files2Delete(castor::stager::Request* req,
                             castor::IClient *client,
                             castor::Services* svcs,
                             castor::stager::IGCSvc* gcSvc,
                             castor::BaseAddress &ad) {
      // Usefull Variables
      char *func =  "castor::stager::Files2Delete";
      std::string error;
      castor::stager::Files2Delete *uReq;
      std::vector<castor::stager::GCLocalFile*>* result = 0;

      try {

        /* get the Files2Delete */
        /* -------------------- */
        // cannot return 0 since we check the type before calling this method
        uReq = dynamic_cast<castor::stager::Files2Delete*> (req);

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking selectFiles2Delete");
        result = gcSvc->selectFiles2Delete(uReq->diskServer());

      } catch (castor::exception::Exception e) {
        serrno = e.code();
        error = e.getMessage().str();
        STAGER_LOG_DB_ERROR(NULL, func,
                            e.getMessage().str().c_str());
      }

      /* Build the response             */
      /* ------------------------------ */
      STAGER_LOG_DEBUG(NULL, "Building Response");
      castor::rh::GCFilesResponse res;
      if (0 != serrno) {
        res.setErrorCode(serrno);
        res.setErrorMessage(error);
      } else {
        for(std::vector<castor::stager::GCLocalFile *>::iterator it =
              result->begin();
            it != result->end();
            it++) {
          // Here we transfer the ownership of the GCLocalFiles
          // to res. Result can thus be deleted with no risk
          // of memory leak
          res.addFiles(*it);
        }
      }

      /* Reply To Client                */
      /* ------------------------------ */
      replyToClient(client, &res, req->reqId());

      /* Cleanup                        */
      /* ------------------------------ */
      if (result) delete result;
    }

  } // End of namespace stager

} // End of namespace castor


/* -------------------------------------- */
/* stager_gc_process()                    */
/*                                        */
/* Purpose: Gc 'process' part of service  */
/*                                        */
/* Input:  (void *) output    Selection   */
/*                                        */
/* Output: n/a                            */
/*                                        */
/* Return: 0 [OK] or -1 [ERROR, serrno]   */
/* -------------------------------------- */
EXTERN_C int DLL_DECL stager_gc_process(void *output) {

  char *func =  "stager_gc_process";
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
  castor::stager::IGCSvc *gcSvc = 0;
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
      svcs->service("DbGCSvc", castor::SVC_DBGCSVC);
    gcSvc = dynamic_cast<castor::stager::IGCSvc*>(svc);

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
    STAGER_LOG_DB_ERROR(NULL,"stager_gc_process",
                        e.getMessage().str().c_str());
    if (req) delete req;
    if (gcSvc) gcSvc->release();
    return -1;
  }

  /* ===========================================================
   *
   * 2) CALLING PHASE
   * At this point we can send a reply to the client
   * We get prepared to call the method
   */

  switch (req->type()) {

  case castor::OBJ_FilesDeleted:
  case castor::OBJ_FilesDeletionFailed:
    castor::stager::handle_filesDeletedOrFailed
      (req, client, svcs, gcSvc, ad);
    break;

  case castor::OBJ_Files2Delete:
    castor::stager::handle_files2Delete
      (req, client, svcs, gcSvc, ad);
    break;

  default:
    castor::exception::Internal e;
    e.getMessage() << "Unknown Request type : "
                   << castor::ObjectsIdStrings[req->type()];
    if (req) delete req;
    if (gcSvc) gcSvc->release();
    throw e;
  }

  // Delete Request From the database
  svcs->deleteRep(&ad, req, true);

  // Cleanup
  if (req) delete req;
  if (gcSvc) gcSvc->release();
  STAGER_LOG_RETURN(serrno == 0 ? 0 : -1);
}
