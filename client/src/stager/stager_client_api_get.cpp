/*
 * $Id: stager_client_api_get.cpp,v 1.11 2004/12/03 17:20:04 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_get.cpp,v $ $Revision: 1.11 $ $Date: 2004/12/03 17:20:04 $ CERN IT-ADC/CA Benjamin Couturier";
#endif

/* ============== */
/* System headers */
/* ============== */

/* ============= */
/* Local headers */
/* ============= */
#include "errno.h"
#include "serrno.h"
#include "stager_client_api.h"
#include "stager_admin_api.h"
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/StageGetRequest.hpp"
#include "castor/stager/StagePrepareToGetRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/FileResponse.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Communication.hpp"

/* ================= */
/* External routines */
/* ================= */

/* ================= */
/* stage_prepareToGet*/
/* ================= */




EXTERN_C int DLL_DECL stage_prepareToGet(const char *userTag,
                                  struct stage_prepareToGet_filereq *requests,
                                  int nbreqs,
                                  struct stage_prepareToGet_fileresp **responses,
                                  int *nbresps,
                                  char **requestId,
                                  struct stage_options* opts) {

 
  return 0;



}


/* ================= */
/* StageGet          */
/* ================= */


EXTERN_C int DLL_DECL stage_get(const char *userTag,
				const char *protocol,
				const char *filename,
				struct stage_io_fileresp ** response,
				char **requestId,
				struct stage_options* opts) {
  
  char *func = "stage_get";
  int rc = -1;
  int saved_serrno = 0;
  std::vector<castor::rh::Response *>respvec;

  if (0 == filename
      || 0 ==  response) {
    serrno = EINVAL;
    stage_errmsg(func, "Invalid input parameter");
    return -1;
  }

  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);

    // Uses a BaseClient to handle the request
    castor::client::BaseClient client;
    castor::stager::StageGetRequest req;
    castor::stager::SubRequest *subreq = new castor::stager::SubRequest();
	
    if (userTag) {
      std::string suserTag(userTag);
      req.setUserTag(suserTag);
    }
    
    if (protocol) {
      std::string sprotocol(protocol);
      subreq->setProtocol(sprotocol);
    }

    std::string sfilename(filename);
    subreq->setFileName(sfilename);
    req.addSubRequests(subreq);
    subreq->setRequest(&req);

    // Submitting the request
    castor::client::VectorResponseHandler rh(&respvec);
    std::string reqid = client.sendRequest(&req, &rh);
    
    if (requestId != NULL) {
      *requestId = strdup(reqid.c_str());
    }
 
    // Checking the result
    // Parsing the responses which have been stored in the vector
    int nbResponses =  respvec.size();
    
    if (nbResponses <= 0) {
      castor::exception::Internal e;
      e.getMessage() << "No responses received";
      throw e;
    }

    // Creating the file response
    // Same size as requests as we only do files for the moment
    *response = (struct stage_io_fileresp *) 
      malloc(sizeof(struct stage_io_fileresp));
    
    if (*response == NULL) {
      castor::exception::Exception e(ENOMEM);
      e.getMessage() << "Could not allocate memory for response";
      throw e;
    }
    
    // Casting the response into a FileResponse !
    castor::rh::IOResponse* fr = 
      dynamic_cast<castor::rh::IOResponse*>(respvec[0]);
      if (0 == fr) {
        castor::exception::Exception e(SEINTERNAL);
        e.getMessage() << "Error in dynamic cast, response was NOT a file response";
        throw e;
      }
      (*response)->castor_filename = strdup(fr->castorFileName().c_str());
      (*response)->protocol = strdup(fr->protocol().c_str());
      (*response)->server = strdup(fr->server().c_str());
      (*response)->port = fr->port();
      (*response)->filename = strdup(fr->fileName().c_str());
      (*response)->status = fr->status();
      (*response)->errorCode = fr->errorCode();
      if (fr->errorMessage().length() > 0) {
        (*response)->errorMessage= strdup(fr->errorMessage().c_str());
      } else {
        (*response)->errorMessage=0;
      }
      rc = 0;
      
  } catch (castor::exception::Communication e) {
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    if (requestId != NULL && e.getRequestId().length() > 0) {
      *requestId = strdup(e.getRequestId().c_str());
    }
    rc = -1;
    saved_serrno = e.code();
  } catch (castor::exception::Exception e) {
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    rc = -1;
    saved_serrno = e.code();
  }
  
  // The responses should be deallocated by the API !
  // Only one entry has been put in the vector
  if (respvec.size() > 0 && 0 != respvec[0]) delete respvec[0];
  
  serrno = saved_serrno;
  return rc;

}
