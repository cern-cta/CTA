/*
 * $Id: QueryRequestSvcThread.cpp,v 1.4 2005/01/27 12:38:20 jdurand Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: QueryRequestSvcThread.cpp,v $ $Revision: 1.4 $ $Date: 2005/01/27 12:38:20 $ CERN IT-ADC/CA Ben Couturier";
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
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/stager/StageFindRequestRequest.hpp"
#include "castor/stager/StageRequestQueryRequest.hpp"
#include "castor/stager/RequestQueryType.hpp"
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/rh/FileQueryResponse.hpp"
#include "castor/query/IQuerySvc.hpp"


#undef logfunc

#include "stager_query_service.h"
#include "stager_macros.h"
#include "serrno.h"

#undef NULL
#define NULL 0

/* ------------------------------------- */
/* stager_query_select()                   */
/*                                       */
/* Purpose: Query 'select' part of service */
/*                                       */
/* Input:  n/a                           */
/*                                       */
/* Output:  (void **) output   Selected  */
/*                                       */
/* Return: 0 [OK] or -1 [ERROR, serrno]  */
/* ------------------------------------- */



EXTERN_C int DLL_DECL stager_query_select(void **output) {
  char *func =  "stager_query_select";
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
    STAGER_LOG_VERBOSE(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc =
      svcs->service("OraStagerSvc", castor::SVC_ORASTAGERSVC);
    stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);


    /* Get any new request to do    */
    /* ---------------------------- */
    STAGER_LOG_VERBOSE(NULL,"Getting any request to do");
    std::vector<castor::ObjectsIds> types;
    types.push_back(castor::OBJ_StageFileQueryRequest);
    types.push_back(castor::OBJ_StageFindRequestRequest);
    types.push_back(castor::OBJ_StageRequestQueryRequest);
    castor::stager::Request* req = stgSvc->requestToDo(types);

    if (0 == req) {
      /* Nothing to do */
      STAGER_LOG_VERBOSE(NULL,"Nothing to do");
      serrno = ENOENT;
      rc = -1;
    } else {
      STAGER_LOG_VERBOSE(NULL,"req FOUND");
      *output = req;
      rc = 0;
    }

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    STAGER_LOG_DB_ERROR(NULL,"stager_query_select",
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

    namespace queryService {

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
       * Handles a fileQueryRequest and replies to client.
       * @param req the request to handle
       * @param client the client where to send the response
       * @param svcs the Services object to use
       * @param qrySvc the stager service to use
       * @param ad the address where to load/store objects in the DB
       */
      void handle_fileQueryRequest(castor::stager::Request* req,
                                   castor::IClient *client,
                                   castor::Services* svcs,
                                   castor::query::IQuerySvc* qrySvc,
                                   castor::BaseAddress &ad) {

        // Usefull Variables
        char *func =  "castor::stager::queryService::handle_fileQueryRequest";
        std::string error;
        castor::stager::StageFileQueryRequest *uReq;
        
        try {

          /* get the StageFileQueryRequest */
          /* ----------------------------- */
          // cannot return 0 since we check the type before calling this method
          uReq = dynamic_cast<castor::stager::StageFileQueryRequest*> (req);

          /* Iterating on the parameters to reply to each qry */
          /* ------------------------------------------------ */
          std::vector<castor::stager::QueryParameter*> params =
            uReq->parameters();
          
          if (0 ==  uReq->parameters().size()) { 
            STAGER_LOG_DB_ERROR(NULL,"handle_fileQueryRequest", 
                                "StageFileQueryRequest has no parameters");
          }

          for(std::vector<QueryParameter*>::iterator it = params.begin();
              it != params.end();
              ++it) {
            
            std::string fid, nshost;
            bool queryOk = false;
            
            castor::stager::RequestQueryType ptype = (*it)->queryType();
            std::string pval = (*it)->value();
            
            switch(ptype) {
            case REQUESTQUERYTYPE_FILENAME:
              STAGER_LOG_VERBOSE(NULL, "Looking up file id from filename");
              struct Cns_fileid Cnsfileid;
              memset(&Cnsfileid,'\0',sizeof(Cnsfileid));
              struct Cns_filestat Cnsfilestat;
              if (Cns_statx(pval.c_str(),&Cnsfileid,&Cnsfilestat) != 0) {
                std::stringstream sst;
                sst << Cnsfileid.fileid;
                fid = sst.str();
                nshost = std::string(Cnsfileid.server);
                queryOk = true;  
              } else {
                castor::exception::Exception e(serrno);
                e.getMessage() << pval.c_str() << " not found";
                throw e;
              }
              break;
            case REQUESTQUERYTYPE_REQID:
              break;
            case REQUESTQUERYTYPE_USERTAG:
              break;
            case REQUESTQUERYTYPE_FILEID:
              STAGER_LOG_VERBOSE(NULL, "Received fileif parameter");
              std::string::size_type idx = pval.find('@');
              if (idx == std::string::npos) {
                // XXX Error
              }
              fid = pval.substr(0, idx);
              nshost = pval.substr(idx + 1);
              queryOk = true;
              break;
            }
            
            if (!queryOk) continue;
            /* Invoking the method                */
            /* ---------------------------------- */
            STAGER_LOG_DEBUG(NULL, "Invoking diskCopies4File");
            std::list<castor::stager::DiskCopyForRecall*> result =
              qrySvc->diskCopies4File(fid, nshost);

            if (result.size() == 0) {
              castor::exception::Exception e(ENOENT);
              e.getMessage() << pval.c_str() << " not in stager";
              throw e;
            }

            for(std::list<castor::stager::DiskCopyForRecall*>::iterator dcit 
                  = result.begin();
                dcit != result.end();
                ++dcit) {

              castor::stager::DiskCopyForRecall* diskcopy = *dcit;

              /* Preparing the response */
              /* ---------------------- */
              STAGER_LOG_DEBUG(NULL, "Building Response");
              castor::rh::FileQueryResponse res;
              res.setStatus(diskcopy->status());
              if (ptype ==  REQUESTQUERYTYPE_FILENAME) {
                res.setFileName(pval);
              } else if (ptype ==  REQUESTQUERYTYPE_FILEID) {  
                //res.setFileId(pval);
              }
              
              /* Sending the response */
              /* -------------------- */
              replyToClient(client, &res);
            
            }
            
          }
        } catch (castor::exception::Exception e) {
          serrno = e.code();
          error = e.getMessage().str();
          STAGER_LOG_DB_ERROR(NULL, func,
                              e.getMessage().str().c_str());
          STAGER_LOG_DEBUG(NULL, "Building Response for error");

          // try/catch this as well ?
          /* Send the execption to the client */
          /* -------------------------------- */
          castor::rh::FileQueryResponse res;
          if (0 != serrno) {
            res.setErrorCode(serrno);
            res.setErrorMessage(error);
        
          }
          
          /* Reply To Client                */
          /* ------------------------------ */
          replyToClient(client, &res);

        }  
      }
      
      
      

      /**
       * Handles a findRequestRequest and replies to client.
       * @param req the request to handle
       * @param client the client where to send the response
       * @param svcs the Services object to use
       * @param qrySvc the stager service to use
       * @param ad the address where to load/store objects in the DB
       */
      void handle_findRequestRequest(castor::stager::Request* req,
                                     castor::IClient *client,
                                     castor::Services* svcs,
                                     castor::query::IQuerySvc* qrySvc,
                                     castor::BaseAddress &ad) {
        char *func = "handle_findRequestRequest";
        STAGER_LOG_VERBOSE(NULL,"Handling findRequestRequest");
      }

      /**
       * Handles a requestQueryRequest and replies to client.
       * @param req the request to handle
       * @param client the client where to send the response
       * @param svcs the Services object to use
       * @param qrySvc the stager service to use
       * @param ad the address where to load/store objects in the DB
       */
      void handle_requestQueryRequest(castor::stager::Request* req,
                                      castor::IClient *client,
                                      castor::Services* svcs,
                                      castor::query::IQuerySvc* qrySvc,
                                      castor::BaseAddress &ad) {
        char *func = "handle_requestQueryRequest";
        STAGER_LOG_VERBOSE(NULL,"Handling requestQueryRequest");
      }

    } // End of namespace query service

  } // End of namespace stager

} // End of namespace castor


/* --------------------------------------- */
/* stager_query_process()                  */
/*                                         */
/* Purpose:Query 'process' part of service */
/*                                         */
/* Input:  (void *) output    Selection    */
/*                                         */
/* Output: n/a                             */
/*                                         */
/* Return: 0 [OK] or -1 [ERROR, serrno]    */
/* --------------------------------------- */
EXTERN_C int DLL_DECL stager_query_process(void *output) {

  char *func =  "stager_query_process";
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
  castor::query::IQuerySvc *qrySvc = 0;
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
      svcs->service("OraQuerySvc", castor::SVC_ORAQUERYSVC);
    qrySvc = dynamic_cast<castor::query::IQuerySvc*>(svc);

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

    /* Getting the parameters  */
    /* ----------------------- */
//     castor::stager::QryRequest* qryreq = 
//       dynamic_cast<castor::stager::QryRequest*>(req);
//     if (0 == qryreq) {
//       castor::exception::Internal e;
//       e.getMessage() << "Should be preocessing a QryRequest!";
//       throw e;
//     }
    svcs->fillObj(&ad, req, castor::OBJ_QueryParameter);

  } catch (castor::exception::Exception e) {
    // If we fail here, we do NOT have enough information to
    // reply to the client !
    serrno = e.code();
    STAGER_LOG_DB_ERROR(NULL,"stager_query_select",
                        e.getMessage().str().c_str());
    if (req) delete req;
    if (qrySvc) qrySvc->release();
    return -1;
  }

  /* ===========================================================
   *
   * 2) CALLING PHASE
   * At this point we can send a reply to the client
   * We get prepared to call the method
   */

  switch (req->type()) {

  case castor::OBJ_StageFileQueryRequest:
    castor::stager::queryService::handle_fileQueryRequest
      (req, client, svcs, qrySvc, ad);
    break;

  case castor::OBJ_StageFindRequestRequest:
    castor::stager::queryService::handle_findRequestRequest
      (req, client, svcs, qrySvc, ad);
    break;

  case castor::OBJ_StageRequestQueryRequest:
    castor::stager::queryService::handle_requestQueryRequest
      (req, client, svcs, qrySvc, ad);
    break;

  default:
    castor::exception::Internal e;
    e.getMessage() << "Unknown Request type : "
                   << castor::ObjectsIdStrings[req->type()];
    if (req) delete req;
    if (qrySvc) qrySvc->release();
    throw e;
  }

  // Cleanup
  if (req) delete req;
  if (qrySvc) qrySvc->release();
  STAGER_LOG_RETURN(serrno == 0 ? 0 : -1);
}
