/*
 * $Id: stager_client_api_rm.cpp,v 1.15 2009/07/13 06:22:08 waldron Exp $
 */

/*
 * Copyright (C) 2005 by CERN/IT/ADC/CA
 * All rights reserved
 */

#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/StageRmRequest.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/FileResponse.hpp"
#include "h/serrno.h"
#include "h/stager_client_api.h"
#include "h/stager_client_api_common.hpp"

// To be removed when getting rid of
// request printing
#include "castor/ObjectSet.hpp"

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>


/* ================= */
/* Internal routines */
/* ================= */
static int _processFileRequest(const char *func,
			       castor::stager::FileRequest& req,
			       struct stage_filereq *requests,
			       int nbreqs,
			       struct stage_fileresp **responses,
			       int *nbresps,
			       char **requestId,
			       struct stage_options* opts) {
 int ret=0;
  if (requests == NULL
      || nbreqs <= 0
      || responses == NULL
      || nbresps == NULL) {
    serrno = EINVAL;
    stager_errmsg(func, "Invalid input parameter");
    return -1;
  }

  try {
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());
    ret=setDefaultOption(opts);
    client.setOptions(opts);
    client.setAuthorizationId();
    if(ret==-1){free(opts);}

    // Preparing the requests
    for(int i=0; i<nbreqs; i++) {

      if (!(requests[i].filename)) {
        serrno = EINVAL;
        stager_errmsg(func, "filename in request %d is NULL", i);
        return -1;
      }

      castor::stager::SubRequest *subreq = new castor::stager::SubRequest();
      req.addSubRequests(subreq);
      std::string sfilename(requests[i].filename);
      subreq->setFileName(sfilename);
      subreq->setRequest(&req);

    }

    // Using the VectorResponseHandler which stores everything in
    // A vector. BEWARE, the responses must be de-allocated afterwards
    std::vector<castor::rh::Response *>respvec;
    castor::client::VectorResponseHandler rh(&respvec);
    std::string reqid = client.sendRequest(&req, &rh);

    if (requestId != NULL) {
      *requestId = strdup(reqid.c_str());
    }


    // Parsing the responses which have been stored in the vector
    int nbResponses =  respvec.size();

    if (nbResponses <= 0) {
      // We got not replies, this is not normal !
      serrno = SEINTERNAL;
      stager_errmsg(func, "No responses received");
      return -1;
    }


    // Creating the array of file responses
    // Same size as requests as we only do files for the moment
    *responses = (struct stage_fileresp *) malloc(sizeof(struct stage_fileresp)
                                                  * nbResponses);
    if (*responses == NULL) {
      serrno = ENOMEM;
      stager_errmsg(func, "Could not allocate memory for responses");
      return -1;
    }
    *nbresps = nbResponses;


    for (int i=0; i<(int)respvec.size(); i++) {

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

  } catch (castor::exception::Exception& e) {
    serrno = e.code();
    stager_errmsg(func, (e.getMessage().str().c_str()));
    return -1;
  }

  return 0;
}


/* ================= */
/* External routines */
/* ================= */


////////////////////////////////////////////////////////////
//    stage_rm                                            //
////////////////////////////////////////////////////////////


EXTERN_C int stage_rm(struct stage_filereq *requests,
                                    int nbreqs,
                                    struct stage_fileresp **responses,
                                    int *nbresps,
                                    char **requestId,
                                    struct stage_options* opts) {

  const char *func = "stage_rm";
  castor::stager::StageRmRequest req;

  stage_trace(3, "%s", func);
  return _processFileRequest(func,
			     req,
			     requests,
			     nbreqs,
			     responses,
			     nbresps,
			     requestId,
			     opts);

}
