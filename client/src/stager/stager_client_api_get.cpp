/*
 * $Id: stager_client_api_get.cpp,v 1.9 2004/12/01 10:02:21 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_get.cpp,v $ $Revision: 1.9 $ $Date: 2004/12/01 10:02:21 $ CERN IT-ADC/CA Benjamin Couturier";
#endif

/* ============== */
/* System headers */
/* ============== */

/* ============= */
/* Local headers */
/* ============= */
#include "errno.h"
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

    if (!filename) {
      serrno = EINVAL;
      stage_errmsg(func, "filename is NULL");
      return -1;
    }

    std::string sfilename(filename);
    subreq->setFileName(sfilename);
    req.addSubRequests(subreq);
    subreq->setRequest(&req);

    // Submitting the request
    std::vector<castor::rh::Response *>respvec;    
    castor::client::VectorResponseHandler rh(&respvec);
    std::string reqid = client.sendRequest(&req, &rh);
    
    if (requestId != NULL) {
      *requestId = strdup(reqid.c_str());
    }
 
    // Checking the result
    // Parsing the responses which have been stored in the vector
    int nbResponses =  respvec.size();
    
    if (nbResponses <= 0) {
      // We got not replies, this is not normal !
      serrno = SEINTERNAL;
      stage_errmsg(func, "No responses received");
      return -1;
    }

    // Creating the file response
    // Same size as requests as we only do files for the moment
    *response = (struct stage_io_fileresp *) 
      malloc(sizeof(struct stage_io_fileresp));
    
    if (*response == NULL) {
      serrno = ENOMEM;
      stage_errmsg(func, "Could not allocate memory for responses");
      return -1;
    }
    
    // Casting the response into a FileResponse !
    castor::rh::FileResponse* fr = 
      dynamic_cast<castor::rh::FileResponse*>(respvec[0]);
      if (0 == fr) {
        castor::exception::Exception e(SEINTERNAL);
        e.getMessage() << "Error in dynamic cast, response was NOT a file response";
        throw e;
      }

      (*response)->filename = strdup(fr->castorFileName().c_str());
      (*response)->status = fr->status();
      (*response)->errorCode = fr->errorCode();
      if (fr->errorMessage().length() > 0) {
        (*response)->errorMessage= strdup(fr->errorMessage().c_str());
      } else {
        (*response)->errorMessage=0;
      }
      // The responses should be deallocated by the API !
      delete respvec[0];

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    return -1;
  }
  
  return 0;

}
