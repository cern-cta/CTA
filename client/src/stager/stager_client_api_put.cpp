/*
 * $Id: stager_client_api_put.cpp,v 1.3 2004/11/19 18:31:32 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_put.cpp,v $ $Revision: 1.3 $ $Date: 2004/11/19 18:31:32 $ CERN IT-ADC/CA Benjamin Couturier";
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
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/StagePutRequest.hpp"
#include "castor/stager/StagePutDoneRequest.hpp"
#include "castor/stager/StageUpdateFileStatusRequest.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/FileResponse.hpp"

// To be removed when getting rid of 
// request printing
#include "castor/ObjectSet.hpp"


/* ================= */
/* Internal routines */
/* ================= */


/* ================= */
/* response Handlers */
/* ================= */

/**
 * StagePut Response Handler
 */
class PutResponseHandler : public castor::client::IResponseHandler {

 public:

  PutResponseHandler() {
    // Already preparing the stageput rsponse stucture
    m_response = (struct stage_put_fileresp *)
      calloc(sizeof(struct stage_put_fileresp), 1);

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

  
  struct stage_put_fileresp *m_response;
};




/* ================= */
/* External routines */
/* ================= */

EXTERN_C int DLL_DECL stage_prepareToPut(const char *userTag,
					struct stage_prepareToPut_filereq *requests,
					int nbreqs,
					struct stage_prepareToPut_fileresp **responses,
					int *nbresps,
					char **requestId,
					 struct stage_options* opts) {

  castor::BaseObject::initLog("", castor::SVC_NOMSG);

}



EXTERN_C int DLL_DECL stage_put(const char *userTag,
				const char *protocol,
				const char *filename,
				int mode,
				struct stage_put_fileresp ** response,
				char **requestId,
				struct stage_options* opts) {
  
  char *func = "stage_put";

  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
    PutResponseHandler rh;
	
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client;
    castor::stager::StagePutRequest req;
    castor::stager::SubRequest *subreq = new castor::stager::SubRequest();

    std::string suserTag(userTag);
    std::string sprotocol(protocol);
    std::string sfilename(filename);

    req.setUserTag(suserTag);
    subreq->setProtocol(sprotocol);
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


////////////////////////////////////////////////////////////
//    stage_putDone                                       //
////////////////////////////////////////////////////////////


/**
 * StagePut Response Handler
 */
class PutDoneResponseHandler : public castor::client::IResponseHandler {

 public:

  PutDoneResponseHandler(struct stage_fileresp *resps,
			 int *nbresps) : m_currentNbResps(0) {
			   // Already preparing the stageput rsponse stucture
    m_resps = resps;
    m_nbResps = nbresps;
    if (m_resps == NULL) {
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << "Response array should already have been allocated";
      throw e;
    }
  }

  virtual void handleResponse(castor::rh::Response& r)
    throw (castor::exception::Exception) {

    m_currentNbResps++;

    if (m_currentNbResps > *m_nbResps) {
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << "Receiving more responses than requests were sent";
      throw e;
    }
    
    // Casting the response into a FileResponse !
    castor::rh::FileResponse* fr = dynamic_cast<castor::rh::FileResponse*>(&r);
    if (0 == fr) {
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << "Error in dynamic cast, response was NOT a file response";
      throw e;
    }

    m_resps[m_currentNbResps-1].filename = strdup(fr->castorFileName().c_str());
    m_resps[m_currentNbResps-1].status = fr->status();
    m_resps[m_currentNbResps-1].errorCode = fr->errorCode();
    if (fr->errorMessage().length() > 0) {
      m_resps[m_currentNbResps-1].errorMessage= strdup(fr->errorMessage().c_str());
    } else {
      m_resps[m_currentNbResps-1].errorMessage=0;
    }

    /* 
    std::cout << "Response:"; 
    castor::ObjectSet set; 
    r.print(std::cout, "", set); 
    std::cout << std::endl;
    */
  };

  virtual void terminate()
    throw (castor::exception::Exception) {
    *m_nbResps  = m_currentNbResps;
  };

  int *m_nbResps;
  int m_currentNbResps;
  struct stage_fileresp *m_resps;
};


EXTERN_C int DLL_DECL stage_putDone _PROTO((struct stage_filereq *requests,
					    int nbreqs,
					    struct stage_fileresp **responses,
					    int *nbresps,
					    char **requestId,
					    struct stage_options* opts)) {

  char *func = "stage_putDone";
  
  if (requests == NULL
      || nbreqs <= 0
      || responses == NULL
      || nbresps == NULL) {
    serrno = EINVAL;
    stage_errmsg(func, "Invalid input parameter");
    return -1;
  }


  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
	
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client;
    castor::stager::StagePutDoneRequest req;

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

    // Creating the array of file responses
    // Same size as requests as we only do files for the moment
    *responses = (struct stage_fileresp *) malloc(sizeof(struct stage_fileresp) * nbreqs);
    if (*responses == NULL) {
	serrno = ENOMEM;
	stage_errmsg(func, "Could not allocate memory for responses");
	return -1;
    }
    *nbresps = nbreqs;

    PutDoneResponseHandler rh(*responses, nbresps);


    client.sendRequest(&req, &rh);

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    return -1;
  }
  
  return 0;



}


////////////////////////////////////////////////////////////
//    stage_updateFileStatus                              //
////////////////////////////////////////////////////////////


EXTERN_C int DLL_DECL stage_updateFileStatus _PROTO((struct stage_filereq *requests,
						     int nbreqs,
						     struct stage_fileresp **responses,
						     int *nbresps,
						     char **requestId,
						     struct stage_options* opts)) {


}
