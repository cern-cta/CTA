/*
 * $Id: stager_client_api_update.cpp,v 1.13 2007/02/21 09:46:22 sponcec3 Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/* ============== */
/* System headers */
/* ============== */
#include <sys/types.h>
#include <fcntl.h>

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
#include "castor/stager/StagePrepareToUpdateRequest.hpp"
#include "castor/stager/StageUpdateRequest.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/FileResponse.hpp"
#include "castor/rh/IOResponse.hpp"
#include "stager_client_api_common.hpp"

// To be removed when getting rid of 
// request printing
#include "castor/ObjectSet.hpp"


/* ================= */
/* Internal routines */
/* ================= */

/* ================= */
/* External routines */
/* ================= */

EXTERN_C int DLL_DECL stage_prepareToUpdate(const char *userTag,
					struct stage_prepareToUpdate_filereq *requests,
					int nbreqs,
					struct stage_prepareToUpdate_fileresp **responses,
					int *nbresps,
					char **requestId,
					 struct stage_options* opts) {

  char *func = "stage_prepareToUpdate";
  int ret=0;
 
  if (requests == NULL
      || nbreqs <= 0
      || responses == NULL
      || nbresps == NULL) {
    serrno = EINVAL;
    stage_errmsg(func, "Invalid input parameter");
    return -1;
  }

  const char *duserTag = (userTag != 0)?userTag:"NULL";
  stage_trace(3, "%s Usertag=%s", func, duserTag);

  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
	
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());
    castor::stager::StagePrepareToUpdateRequest req;

    castor::stager::RequestHelper reqh(&req);  
    ret=setDefaultOption(opts);
    reqh.setOptions(opts);
    client.setOption(opts);
    if (ret==-1){free(opts);}

   if (0 != userTag) {
      req.setUserTag(std::string(userTag));
    }    
    
    // Preparing the requests
    for(int i=0; i<nbreqs; i++) {
      castor::stager::SubRequest *subreq = new castor::stager::SubRequest();
      
      if (!(requests[i].filename)) {
        serrno = EINVAL;
        stage_errmsg(func, "filename in request %d is NULL", i);
        return -1;
      }

      req.addSubRequests(subreq);
      std::string sfilename(requests[i].filename);
      subreq->setFileName(sfilename);
      subreq->setRequest(&req);

    }

    // Using the VectorResponseHandler which stores everything in
    // A vector. BEWARE, the responses must be de-allocated afterwards
    std::vector<castor::rh::Response *>respvec;    
    castor::client::VectorResponseHandler rh(&respvec);
    client.sendRequest(&req, &rh);

    // Parsing the responses which have been stored in the vector
    int nbResponses =  respvec.size();

    if (nbResponses <= 0) {
      // We got not replies, this is not normal !
      serrno = SEINTERNAL;
      stage_errmsg(func, "No responses received");
      return -1;
    }
    

    // Creating the array of file responses
    // Same size as requests as we only do files for the moment
    *responses = (struct stage_prepareToUpdate_fileresp *) 
      malloc(sizeof(struct stage_prepareToUpdate_fileresp) * nbResponses);

    if (*responses == NULL) {
      serrno = ENOMEM;
      stage_errmsg(func, "Could not allocate memory for responses");
      return -1;
    }
    *nbresps = nbResponses;
    

    for (unsigned int i=0; i<respvec.size(); i++) {

      // Casting the response into a FileResponse !
      castor::rh::FileResponse* fr = 
        dynamic_cast<castor::rh::FileResponse*>(respvec[i]);
      if (0 == fr) {
        castor::exception::Exception e(SEINTERNAL);
        e.getMessage() << "Error in dynamic cast, response was NOT a file response";
        throw e;
      }

      (*responses)[i].filename = strdup(fr->castorFileName().c_str());
      (*responses)[i].status = fr->status();
      (*responses)[i].errorCode = fr->errorCode();
      if (fr->errorMessage().length() > 0) {
        (*responses)[i].errorMessage= strdup(fr->errorMessage().c_str());
      } else {
        (*responses)[i].errorMessage=0;
      }
      // The responses should be deallocated by the API !
      delete respvec[i];
    } // for
    
  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    return -1;
  }
  
  return 0;


}



EXTERN_C int DLL_DECL stage_update(const char *userTag,
                                   const char *protocol,
                                   const char *filename,
                                   int flags,
                                   mode_t mode,
				   u_signed64 size,
                                   struct stage_io_fileresp ** response,
                                   char **requestId,
                                   struct stage_options* opts) {
  
  char *func = "stage_update";
 int ret=0;

  if (0 == filename
      || 0 ==  response) {
    serrno = EINVAL;
    stage_errmsg(func, "Invalid input parameter");
    return -1;
  }

  const char *duserTag = (userTag != 0)?userTag:"NULL";
  const char *dprotocol = (protocol != 0)?protocol:"NULL";
  stage_trace(3, "%s Usertag=%s Protocol=%s File=%s mode=%d/size=%llu/flags=%d", 
	      func, duserTag, dprotocol, filename, mode, size, flags);
    
  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);

    // Preparing the request
    castor::client::BaseClient client(stage_getClientTimeout());
    castor::stager::StageUpdateRequest req;
    castor::stager::SubRequest *subreq = new castor::stager::SubRequest();

    if (0 != userTag) {
      req.setUserTag(std::string(userTag));
    }

    castor::stager::RequestHelper reqh(&req);
    ret=setDefaultOption(opts);
    reqh.setOptions(opts);
    client.setOption(opts);
    if (ret==-1){free(opts);}  
    if (0 != protocol) {
      subreq->setProtocol(protocol);
    }

    subreq->setFileName(filename);
    req.addSubRequests(subreq);
    subreq->setRequest(&req);
    subreq->setXsize(size);
    subreq->setModeBits(mode);
    subreq->setFlags(flags);

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

