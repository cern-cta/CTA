/*
 * $Id: stager_client_api_get.cpp,v 1.3 2004/11/09 10:42:07 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_get.cpp,v $ $Revision: 1.3 $ $Date: 2004/11/09 10:42:07 $ CERN IT-ADC/CA Benjamin Couturier";
#endif

/* ============== */
/* System headers */
/* ============== */

/* ============= */
/* Local headers */
/* ============= */
#include "stager_client_api.h"
#include "stager_admin_api.h"
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/StageGetRequest.hpp"
#include "castor/stager/StagePrepareToGetRequest.hpp"
#include "castor/stager/SubRequest.hpp"

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
					char **requestId) {

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



/**
 * StageGet Response Handler for Root files
 */
class GetResponseHandler : public castor::client::IResponseHandler {

 public:

  GetResponseHandler() {
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
  };

  virtual void terminate()
    throw (castor::exception::Exception) {
  };

  
  struct stage_get_fileresp *m_response;
};




EXTERN_C int DLL_DECL stage_get(const char *userTag,
				const char *protocol,
				const char *filename,
				struct stage_get_fileresp ** response,
				char **requestId) {
  
  char *func = "stage_get";

  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
    GetResponseHandler rh;
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

    client.sendRequest(&req, &rh);

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    return -1;
  }
  
  return 0;

}
