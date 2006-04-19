/*
 * $Id: stager_client_api_next.cpp,v 1.3 2006/04/19 12:31:06 gtaur Exp $
 */

/*
 * Copyright (C) 2005 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_next.cpp,v $ $Revision: 1.3 $ $Date: 2006/04/19 12:31:06 $ CERN IT-ADC/CA Benjamin Couturier";
#endif

/* ============== */
/* System headers */
/* ============== */
#include <sys/types.h>
#include <sys/stat.h>

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
#include "castor/stager/RequestHelper.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/ReqIdRequest.hpp"
#include "castor/stager/StageGetNextRequest.hpp"
#include "castor/stager/StagePutNextRequest.hpp"
#include "castor/stager/StageUpdateNextRequest.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/FileResponse.hpp"
#include "castor/rh/IOResponse.hpp"

// To be removed when getting rid of 
// request printing
#include "castor/ObjectSet.hpp"


/* ================= */
/* Internal routines */
/* ================= */
static int _processReqIdRequest(char *func,
				castor::stager::ReqIdRequest& req,
				const char *prevRequestId,
				struct stage_io_fileresp ** response,
				struct stage_options* opts) {

  if (prevRequestId == NULL) {    serrno = EINVAL;
    stage_errmsg(func, "Invalid input parameter");
    return -1;
  }


  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
	
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());

    castor::stager::RequestHelper reqh(&req);
    client.setOption(opts);
    
    req.setParentUuid(std::string(prevRequestId));

    // Using the VectorResponseHandler which stores everything in
    // A vector. BEWARE, the responses must be de-allocated afterwards
    std::vector<castor::rh::Response *>respvec;    
    castor::client::VectorResponseHandler rh(&respvec);
    std::string reqid = client.sendRequest(&req, &rh);


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
      // The responses should be deallocated by the API !
      delete respvec[0];

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    return -1;
  }
  
  return 0;
}


/* ================= */
/* External routines */
/* ================= */

		       
////////////////////////////////////////////////////////////
//    stage_getNext                                       //
////////////////////////////////////////////////////////////
		       
EXTERN_C int stage_getNext(const char *reqId,
			   struct stage_io_fileresp ** response,
			   struct stage_options* opts) {

  char *func = "stage_getNext";
  castor::stager::StageGetNextRequest req;
  int rc =  _processReqIdRequest(func, req, reqId, response, opts);
  return rc;
}


////////////////////////////////////////////////////////////
//    stage_updateNext                                    //
////////////////////////////////////////////////////////////


EXTERN_C int stage_updateNext(const char *reqId,
			      struct stage_io_fileresp ** response,
			      struct stage_options* opts) {
  char *func = "stage_updateNext";
  castor::stager::StageUpdateNextRequest req;
  int rc =  _processReqIdRequest(func, req, reqId, response, opts);
  return rc;
}



////////////////////////////////////////////////////////////
//    stage_putNext                                       //
////////////////////////////////////////////////////////////
		       


EXTERN_C int stage_putNext(const char *reqId,
			   struct stage_io_fileresp ** response,
			   struct stage_options* opts) {
  char *func = "stage_putNext";
  castor::stager::StagePutNextRequest req;
  int rc =  _processReqIdRequest(func, req, reqId, response, opts);
  return rc;


}
