/*
 * $Id: stager_client_api_get.cpp,v 1.7 2004/11/23 15:34:15 sponcec3 Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_get.cpp,v $ $Revision: 1.7 $ $Date: 2004/11/23 15:34:15 $ CERN IT-ADC/CA Benjamin Couturier";
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


/**
 * StageGet Response Handler for Root files
 */
class PrepareToGetResponseHandler : public castor::client::IResponseHandler {

 public:

  PrepareToGetResponseHandler() {
    // Already preparing the stageget rsponse stucture
    m_response = (struct stage_get_fileresp *)
      calloc(sizeof(struct stage_get_fileresp), 1);

    if (m_response == NULL) {
      castor::exception::Exception e(ENOMEM);
      e.getMessage() << "Could not allocate memory for response";
      throw e;
    }
  }

  virtual void handleResponse(castor::rh::Response& r)
    throw (castor::exception::Exception) {
    std::cout << "Handling response !" << std::endl;

     
    std::cout << "Response:"; 
    castor::ObjectSet set; 
    r.print(std::cout, "", set); 
    std::cout << std::endl;
    

  };

  virtual void terminate()
    throw (castor::exception::Exception) {
  };

  
  struct stage_get_fileresp *m_response;
};



EXTERN_C int DLL_DECL stage_prepareToGet(const char *userTag,
					struct stage_prepareToGet_filereq *requests,
					int nbreqs,
					struct stage_prepareToGet_fileresp **responses,
					int *nbresps,
					char **requestId,
					 struct stage_options* opts) {

  castor::BaseObject::initLog("", castor::SVC_NOMSG);
  
  char *func = "stage_prepareToGet";

  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
    PrepareToGetResponseHandler rh;

    // Uses a BaseClient to handle the request
    castor::client::BaseClient client;
    castor::stager::StagePrepareToGetRequest req;

    if (userTag) {
      std::string suserTag(userTag);
      req.setUserTag(suserTag);
    }


    for(int i=0; i<nbreqs; i++) {
      castor::stager::SubRequest *subreq = new castor::stager::SubRequest();

      if (requests[i].protocol) {
	std::string sprotocol(requests[i].protocol);
	subreq->setProtocol(sprotocol);
      }    

      if (!(requests[i].filename)) {
	serrno = EINVAL;
	stage_errmsg(func, "filename in request %d is NULL", i);
	return -1;
      }

      req.addSubRequests(subreq);
      std::string sfilename(requests[i].filename);
      subreq->setFileName(sfilename);
      subreq->setPriority(requests[i].priority);
      subreq->setRequest(&req);

    }

    client.sendRequest(&req, &rh);

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    return -1;
  }
  
  return 0;



}


/* ================= */
/* StageGet          */
/* ================= */


EXTERN_C int DLL_DECL stage_get(const char *userTag,
				const char *protocol,
				const char *filename,
				struct stage_get_fileresp ** response,
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
    client.sendRequest(&req, &rh);
    
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
    *response = (struct stage_get_fileresp *) 
      malloc(sizeof(struct stage_get_fileresp));
    
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
