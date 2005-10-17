/*
 * $Id: QueryRequestSvcThread.cpp,v 1.26 2005/10/17 07:41:57 itglp Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: QueryRequestSvcThread.cpp,v $ $Revision: 1.26 $ $Date: 2005/10/17 07:41:57 $ CERN IT-ADC/CA Ben Couturier";
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
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/BaseObject.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/stager/StageFindRequestRequest.hpp"
#include "castor/stager/StageRequestQueryRequest.hpp"
#include "castor/stager/RequestQueryType.hpp"
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/DiskCopyInfo.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/rh/FileQueryResponse.hpp"
#include "castor/query/IQuerySvc.hpp"
#include "stager_client_api.h"


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
  castor::Services *svcs = 0;
  castor::query::IQuerySvc *qrySvc = 0;

  try {

    /* Loading services */
    /* ---------------- */
    STAGER_LOG_VERBOSE(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc = svcs->service("DbQuerySvc", castor::SVC_DBQUERYSVC);
    qrySvc = dynamic_cast<castor::query::IQuerySvc*>(svc);


    /* Get any new request to do    */
    /* ---------------------------- */
    STAGER_LOG_VERBOSE(NULL,"Getting any request to do");
    castor::stager::Request* req = qrySvc->requestToDo();

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
  qrySvc->release();

  // Return
  STAGER_LOG_RETURN(rc);
}


namespace castor {

  namespace stager {

    namespace queryService {


      /** Dummy status code for non existing files */
      int naStatusCode = 10000;



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
        } catch (castor::exception::Exception e) {
          serrno = e.code();
          STAGER_LOG_DB_ERROR(NULL, func,
                              e.getMessage().str().c_str());
        }
      }


      void sendEndResponse(castor::IClient* client) {
        char *func =  "castor::stager::sendEndResponse";
        try {
          STAGER_LOG_DEBUG(NULL, "Sending End Response");
          castor::replier::RequestReplier *rr =
            castor::replier::RequestReplier::getInstance();
          rr->sendEndResponse(client);
        } catch (castor::exception::Exception e) {
          serrno = e.code();
          STAGER_LOG_DB_ERROR(NULL, func,
                              e.getMessage().str().c_str());
        }
      }


      void setFileResponseStatus(castor::rh::FileQueryResponse* fr,
                                 castor::stager::DiskCopyInfo*  dc,
                                 bool& foundDiskCopy) {
        char *func = "setFileResponseStatus";

        // 1. Mapping diskcopy/tapecopy/segment status to one file status
        stage_fileStatus st = FILE_INVALID_STATUS;
        std::string diskServer = "";

        switch(dc->diskCopyStatus()) {
          // Just IGNORE the discopies in those statuses !
        case DISKCOPY_WAITDISK2DISKCOPY:
        case DISKCOPY_DELETED:
        case DISKCOPY_FAILED:
        case DISKCOPY_GCCANDIDATE:
        case DISKCOPY_BEINGDELETED:
        case DISKCOPY_INVALID:
          return;

        case DISKCOPY_WAITTAPERECALL:
          st = FILE_STAGEIN;
          break;

        case  DISKCOPY_STAGED:
          st = FILE_STAGED;
          diskServer = dc->diskServer();
          break;

        case DISKCOPY_WAITFS:
        case DISKCOPY_STAGEOUT:

          /* fprintf(stderr, "---------> DC %d TC %d Seg %d\n",
             dc->diskCopyStatus(),
             dc->tapeCopyStatus(),
             dc->segmentStatus()); */

          st = FILE_STAGEOUT;
          diskServer = dc->diskServer();
          /*
          switch (dc->tapeCopyStatus()) {
          case -1:
            st =  FILE_STAGEOUT;
            diskServer = dc->diskServer();
            break;
          case TAPECOPY_CREATED:
            st = FILE_CANBEMIGR;
            diskServer = dc->diskServer();
            break;
          case TAPECOPY_TOBEMIGRATED:
          case TAPECOPY_WAITINSTREAMS:
            st = FILE_WAITINGMIGR;
            diskServer = dc->diskServer();
            break;
          case TAPECOPY_SELECTED:
            if (dc->segmentStatus() == SEGMENT_FAILED) {
              st = FILE_PUTFAILED;
            } else {
              st = FILE_BEINGMIGR;
              diskServer = dc->diskServer();
            }
            break;
          }
          */
          break;

        case DISKCOPY_CANBEMIGR:
          st =  FILE_CANBEMIGR;
          diskServer = dc->diskServer();
          break;

        }

        // 2. Aggregate status for the various diskcopies
        if (!foundDiskCopy) {
          STAGER_LOG_DEBUG(NULL, "Setting diskServer");
          STAGER_LOG_DEBUG(NULL, diskServer.c_str());
          fr->setStatus(st);
          fr->setDiskServer(diskServer);
          foundDiskCopy = true;
        } else {
          // If there are sevral diskcopies for the file
          // set the status to staged if ANY of the disk copies is
          // staged, otherwise keep the original status.
          if (dc->diskCopyStatus() == DISKCOPY_STAGED) {
            fr->setStatus(FILE_STAGED);
            fr->setDiskServer(dc->diskServer());
            STAGER_LOG_DEBUG(NULL, "Setting diskServer");
            STAGER_LOG_DEBUG(NULL, diskServer.c_str());
          }
        }
      }


      void handle_fileQueryRequest_byFileId(castor::query::IQuerySvc* qrySvc,
                                            castor::IClient *client,
                                            std::string &fid,
                                            std::string &nshost,
                                            u_signed64 svcClassId) {

        char *func =  "castor::stager::queryService::handle_fileQueryRequest_byFileId";

        /* Invoking the method                */
        /* ---------------------------------- */
        STAGER_LOG_DEBUG(NULL, "Invoking diskCopies4File");
        std::list<castor::stager::DiskCopyInfo*> result =
          qrySvc->diskCopies4File(fid, nshost, svcClassId);

        if (result.size() == 0) {
          castor::exception::Exception e(ENOENT);
          e.getMessage() << "File " << fid << "@"
                         << nshost << " not in stager";
          throw e;
        }

        castor::rh::FileQueryResponse res;
        std::ostringstream sst;
        sst << fid << "@" <<  nshost;
        res.setFileName(sst.str());
        bool foundDiskCopy = false;

        for(std::list<castor::stager::DiskCopyInfo*>::iterator dcit
              = result.begin();
            dcit != result.end();
            ++dcit) {

          castor::stager::DiskCopyInfo* diskcopy = *dcit;
          /* Preparing the response */
          /* ---------------------- */
          res.setSize(diskcopy->size());
          setFileResponseStatus(&res, diskcopy, foundDiskCopy);
        }

        // INVALID status is like nothing
        if (res.status() == FILE_INVALID_STATUS) {
          castor::exception::Exception e(ENOENT);
          e.getMessage() << "File " << fid << "@"
                         << nshost << " not in stager";
          throw e;
        }

        /* Sending the response */
        /* -------------------- */
        replyToClient(client, &res);

        /* Cleanup */
        /* ------- */
        for(std::list<castor::stager::DiskCopyInfo*>::iterator dcit
              = result.begin();
            dcit != result.end();
            ++dcit) {
          delete *dcit;
        }

      }




      void handle_fileQueryRequest_byRequest(castor::query::IQuerySvc* qrySvc,
                                             castor::IClient *client,
                                             std::string &val,
                                             u_signed64 svcClassId,
                                             bool byUsertag = false) {
        char *func =  "castor::stager::queryService::handle_fileQueryRequest_byRequest";

        // Performing the query on the database
        std::list<castor::stager::DiskCopyInfo*> result;

        if (byUsertag) {
          result = qrySvc->diskCopies4Usertag(val, svcClassId);
          if (result.size() == 0) {
            castor::exception::Exception e(ENOENT);
            e.getMessage() << "Could not find request for usertag " << val;
            throw e;
          }
        } else {
          result = qrySvc->diskCopies4Request(val, svcClassId);
          if (result.size() == 0) {
            castor::exception::Exception e(ENOENT);
            e.getMessage() << "Could not find request for id " << val;
            throw e;
          }
        }

        /*
         * Iterates over the list of disk copies, and returns one result
         * per fileid to the client. It needs the SQL statement to return the diskcopies
         * sorted by fileid/nshost.
         */

        u_signed64 fileid = 0;
        std::string nshost = "";
        castor::rh::FileQueryResponse res;
        bool foundDiskCopy = false;


        for(std::list<castor::stager::DiskCopyInfo*>::iterator dcit
              = result.begin();
            dcit != result.end();
            ++dcit) {

          castor::stager::DiskCopyInfo* diskcopy = *dcit;

          if (diskcopy->fileId() != fileid
              || diskcopy->nsHost() != nshost) {

            // Sending the response for the previous
            // castor file being processed
            if (foundDiskCopy) {
              replyToClient(client, &res);
            }

            // Now processing a new file !
            bool foundDiskCopy = false;
            fileid = diskcopy->fileId();
            nshost = diskcopy->nsHost();


            std::ostringstream sst;
            sst << diskcopy->fileId() << "@" <<  diskcopy->nsHost();
            res.setFileName(sst.str());
            res.setSize(diskcopy->size());

          }

          /* Preparing the response */
          /* ---------------------- */
          setFileResponseStatus(&res, diskcopy, foundDiskCopy);
        }

        // Send the last response if necessary
        if (foundDiskCopy) {
          replyToClient(client, &res);
        }

        /* Cleanup */
        /* ------- */
        for(std::list<castor::stager::DiskCopyInfo*>::iterator dcit
              = result.begin();
            dcit != result.end();
            ++dcit) {
          delete *dcit;
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

            castor::stager::RequestQueryType ptype = (*it)->queryType();
            std::string pval = (*it)->value();

            try {


              std::string fid, nshost, reqidtag;
              int statcode;
              bool queryOk = false;

              switch(ptype) {
              case REQUESTQUERYTYPE_FILENAME:
                STAGER_LOG_DEBUG(NULL, "Looking up file id from filename");
                struct Cns_fileid Cnsfileid;
                memset(&Cnsfileid,'\0',sizeof(Cnsfileid));
                struct Cns_filestat Cnsfilestat;
                statcode =  Cns_statx(pval.c_str(),&Cnsfileid,&Cnsfilestat);
                if (statcode == 0) {
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
                queryOk = true;
                break;
              case REQUESTQUERYTYPE_USERTAG:
                queryOk = true;
                break;
              case REQUESTQUERYTYPE_FILEID:
                STAGER_LOG_DEBUG(NULL, "Received fileid parameter");
                std::string::size_type idx = pval.find('@');
                if (idx == std::string::npos) {
                  fid = pval;
                  nshost = '%';
                } else {
                  fid = pval.substr(0, idx);
                  nshost = pval.substr(idx + 1);
                }
                queryOk = true;
                break;
              }

              if (!queryOk) {
                castor::exception::Exception e(serrno);
                e.getMessage() << "Could not parse parameter: "
                               << ptype << "/"
                               << pval;
                throw e;
              }

              // Get the SvcClass associated to the request
              /*
              castor::stager::SvcClass* svcClass = uReq->svcClass();
              if (0 == svcClass) {
                castor::exception::Internal e;
                e.getMessage() << "found attached with no SvcClass.\n"
                               << uReq;
                throw e;
              }*/

              // This time call the proper handling request
              switch(ptype) {
              case REQUESTQUERYTYPE_FILENAME:
              case REQUESTQUERYTYPE_FILEID:
                STAGER_LOG_DEBUG(NULL, "Calling handle_fileQueryRequest_byFileId");
                handle_fileQueryRequest_byFileId(qrySvc,
                                                 client,
                                                 fid,
                                                 nshost,
                                                 0 /* svcClass */);

                break;
              case REQUESTQUERYTYPE_REQID:
                STAGER_LOG_DEBUG(NULL, "Calling handle_fileQueryRequest_byRequest");
                handle_fileQueryRequest_byRequest(qrySvc,
                                                  client,
                                                  pval,
                                                  0 /* svcClass */,
                                                  false);
                break;
              case REQUESTQUERYTYPE_USERTAG:
                STAGER_LOG_DEBUG(NULL, "Calling handle_fileQueryRequest_byRequest/usertag");
                handle_fileQueryRequest_byRequest(qrySvc,
                                                  client,
                                                  pval,
                                                  0 /* svcClass */,
                                                  true);
                break;

              }


            } catch (castor::exception::Exception e) {
              serrno = e.code();
              error = e.getMessage().str();
              // In case the file did not exist, we don't consider
              // it as an error from the server point of view
              if (e.code() == ENOENT) {
                STAGER_LOG(STAGER_MSG_USAGE, NULL,
                           "STRING", func,
                           "STRING", e.getMessage().str().c_str());
              } else {
                STAGER_LOG_DB_ERROR(NULL, func,
                                    e.getMessage().str().c_str());
              }
              /* Send the execption to the client */
              /* -------------------------------- */
              castor::rh::FileQueryResponse res;
              if (0 != serrno) {
                res.setStatus(naStatusCode);
                res.setFileName(pval);
                res.setErrorCode(serrno);
                res.setErrorMessage(error);
              }

              /* Reply To Client                */
              /* ------------------------------ */
              replyToClient(client, &res);
            } // End catch
          } // End loop on all diskcopies
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
          res.setStatus(naStatusCode);
          if (0 != serrno) {
            res.setErrorCode(serrno);
            res.setErrorMessage(error);

          }

          /* Reply To Client                */
          /* ------------------------------ */
          replyToClient(client, &res);

        }


        sendEndResponse(client);

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

  /* Setting the address to access the db */
  /* ------------------------------------ */
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);

  try {

    /* Loading services */
    /* ---------------- */
    STAGER_LOG_VERBOSE(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc = svcs->service("DbQuerySvc", castor::SVC_DBQUERYSVC);
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

    /* Getting the svcClass */
    /* -------------------- */
    /*
    STAGER_LOG_VERBOSE(NULL, "Getting query's className");
    std::string className = req->svcClassName();
    if ("" == className) {
      STAGER_LOG_DEBUG(NULL,"No className - using \"default\"");
      className = "default";
    }
    castor::stager::SvcClass* svcClass = qrySvc->selectSvcClass(className);
    if (0 == svcClass) {
      castor::exception::InvalidArgument e;
      e.getMessage() << "Invalid className : " << className;
      throw e;
    }

    /* Filling SvcClass in the DataBase */
    /* -------------------------------- *
    req->setSvcClass(svcClass);
    svcs->fillRep(&ad, req, castor::OBJ_SvcClass, true);
    */
    req->setSvcClass(0);

  } catch (castor::exception::Exception e) {
    // If we fail here, we do NOT have enough information to
    // reply to the client !
    serrno = e.code();
    STAGER_LOG_DB_ERROR(NULL,"stager_query_select",
                        e.getMessage().str().c_str());
    if (0 != req) {
      castor::stager::SvcClass *svcClass = req->svcClass();
      if (0 != svcClass) {
        delete svcClass;
      }
      delete req;
    }
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
    if (0 != req) {
      castor::stager::SvcClass *svcClass = req->svcClass();
      if (0 != svcClass) {
        delete svcClass;
      }
      delete req;
    }
    if (qrySvc) qrySvc->release();
    throw e;
  }

  // Delete Request From the database
  svcs->deleteRep(&ad, req, true);

  // Cleanup
  if (0 != req) {
    castor::stager::SvcClass *svcClass = req->svcClass();
    if (0 != svcClass) {
      delete svcClass;
    }
    delete req;
  }
  if (qrySvc) qrySvc->release();
  STAGER_LOG_RETURN(serrno == 0 ? 0 : -1);
}
