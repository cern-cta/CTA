/******************************************************************************
 *                      stager_client_api_listPrivileges.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * api to list privileges i.e. the content of the black and white list
 *
 * @author sponcec3
 *****************************************************************************/

// includes
#include "errno.h"
#include "stager_client_api.h"
#include "stager_client_api_common.hpp"
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/bwlist/ListPrivilegesResponse.hpp"
#include "castor/bwlist/ListPrivileges.hpp"
#include "castor/bwlist/Privilege.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/exception/Internal.hpp"
#include <vector>
#include <string.h>

//------------------------------------------------------------------------------
// ListPrivilegesResponseHandler
//------------------------------------------------------------------------------
/**
 * A dedicated little response handler for the ListPrivileges
 * requests
 */
class ListPrivilegesResponseHandler : public castor::client::IResponseHandler {
public:
  ListPrivilegesResponseHandler
  (std::vector<castor::bwlist::Privilege*>* result) :
    m_result(result) {}

  virtual void handleResponse(castor::rh::Response& r)
    throw (castor::exception::Exception) {
    if (0 != r.errorCode()) {
      castor::exception::Exception e(r.errorCode());
      e.getMessage() << r.errorMessage();
      throw e;
    }
    castor::bwlist::ListPrivilegesResponse *resp =
      dynamic_cast<castor::bwlist::ListPrivilegesResponse*>(&r);
    if (0 == resp) {
      castor::exception::Internal e;
      e.getMessage() << "Could not cast response into ListPrivilegesResponse";
      throw e;
    }
    for (std::vector<castor::bwlist::Privilege*>::iterator
           it = resp->privileges().begin();
         it != resp->privileges().end();
         it++) {
      // Here we transfer the ownership of the ListPrivileges
      // from resp to m_result. So the resp can be cleared
      // without any memory leak. It is even mandatory.
      m_result->push_back(*it);
    }
    // we clear the response as explained above
    resp->privileges().clear();
  };
  virtual void terminate()
    throw (castor::exception::Exception) {};
private:
  // where to store the diskCopy
  std::vector<castor::bwlist::Privilege*>* m_result;
};

//-----------------------------------------------------------------------------
// stage_listPrivileges
//-----------------------------------------------------------------------------
EXTERN_C int stage_listPrivileges
(int user,
 int group,
 unsigned int requestType,
 struct stage_listpriv_resp** privileges,
 int* nbPrivs,
 struct stage_options* opts) {
  try {
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());
    int ret = setDefaultOption(opts);
    client.setOptions(opts);
    client.setAuthorizationId();
    if (ret == -1) { free(opts); }

    // create request
    castor::bwlist::ListPrivileges req;
    req.setUserId(user);
    req.setGroupId(group);
    req.setRequestType(requestType);

    // Send request
    std::vector<castor::bwlist::Privilege *>respvec;
    ListPrivilegesResponseHandler rh(&respvec);
    client.sendRequest(&req, &rh);

    // Parsing the responses which have been stored in the vector
    int nbResponses =  respvec.size();
    *nbPrivs = nbResponses;
    if (nbResponses > 0) {
      *privileges =
        (struct stage_listpriv_resp*) malloc
        (sizeof(struct stage_listpriv_resp) * nbResponses);
      if (*privileges == NULL) {
        serrno = ENOMEM;
        stager_errmsg("stage_listPrivileges",
                      "Could not allocate memory for privileges");
        return -1;
      }
      for (int i = 0; i < (int)respvec.size(); i++) {
        (*privileges)[i].svcClass = strdup(respvec[i]->serviceClass().c_str());
        (*privileges)[i].uid = respvec[i]->euid();
        (*privileges)[i].gid = respvec[i]->egid();
        (*privileges)[i].requestType = respvec[i]->requestType();
        (*privileges)[i].isGranted = respvec[i]->granted();
        delete respvec[i];
      }
    }
  } catch (castor::exception::Exception& e) {
    serrno = e.code();
    stager_errmsg("stage_listPrivileges", (e.getMessage().str().c_str()));
    return -1;
  }
  return 0;
}
