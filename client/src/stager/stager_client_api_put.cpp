/*
 * $Id: stager_client_api_put.cpp,v 1.2 2004/11/08 17:29:40 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_put.cpp,v $ $Revision: 1.2 $ $Date: 2004/11/08 17:29:40 $ CERN IT-ADC/CA Benjamin Couturier";
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
#include "castor/stager/StagePutRequest.hpp"
#include "castor/stager/SubRequest.hpp"


/* ================= */
/* Internal routines */
/* ================= */


/* ================= */
/* response Handlers */
/* ================= */

/**
 * StagePut Response Handler for Root files
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
					char **requestId) {

  castor::BaseObject::initLog("", castor::SVC_NOMSG);

}



EXTERN_C int DLL_DECL stage_put(const char *userTag,
				const char *protocol,
				const char *filename,
				int mode,
				struct stage_put_fileresp ** response,
				char **requestId) {
  
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
