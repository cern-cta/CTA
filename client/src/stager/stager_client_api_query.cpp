/*
 * $Id: stager_client_api_query.cpp,v 1.2 2004/11/08 17:29:40 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_query.cpp,v $ $Revision: 1.2 $ $Date: 2004/11/08 17:29:40 $ CERN IT-ADC/CA Benjamin Couturier";
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
#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/stager/SubRequest.hpp"

/* ================= */
/* External routines */
/* ================= */



////////////////////////////////////////////////////////////
//    stage_filequery                                     //
////////////////////////////////////////////////////////////


/**
 * StageGet Response Handler for Root files
 */
class FileQueryResponseHandler : public castor::client::IResponseHandler {

 public:

  FileQueryResponseHandler() {
    // Already preparing the stageget rsponse stucture
    m_response = (struct stage_filequery_resp *)
      calloc(sizeof(struct stage_filequery_resp), 1);

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

  
  struct stage_filequery_resp *m_response;
};

EXTERN_C int DLL_DECL stage_filequery(struct stage_query_req *requests,
				      int nbreqs,
				      struct stage_filequery_resp **responses,
				      int *nbresps){

  castor::BaseObject::initLog("", castor::SVC_NOMSG);
  
  char *func = "stage_filequery";

  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
    FileQueryResponseHandler rh;

    // Uses a BaseClient to handle the request
    castor::client::BaseClient client;
    castor::stager::StageFileQueryRequest req;

    client.sendRequest(&req, &rh);

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    return -1;
  }
  
  return 0;



}


////////////////////////////////////////////////////////////
//    stage_requestquery                                  //
////////////////////////////////////////////////////////////

EXTERN_C int DLL_DECL stage_requestquery(struct stage_query_req *requests,
					 int nbreqs,
					 struct stage_requestquery_resp **responses,
					 int *nbresps,
					 struct stage_subrequestquery_resp **subresponses,
					 int *nbsubresps) {

}



////////////////////////////////////////////////////////////
//    stage_findrequest                                   //
////////////////////////////////////////////////////////////


EXTERN_C int DLL_DECL stage_findrequest(struct stage_query_req *requests,
					int nbreqs,
					struct stage_findrequest_resp **responses,
					int *nbresps){

}

