/*
 * $Id: stager_client_api_version.cpp,v 1.6 2009/07/13 06:22:08 waldron Exp $
 */

/* ============= */
/* Local headers */
/* ============= */
#include "errno.h"
#include "stager_client_api_common.hpp"
#include "stager_client_api.h"
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/rh/Response.hpp"
#include "castor/query/VersionResponse.hpp"
#include "castor/query/VersionQuery.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Communication.hpp"

/* ================= */
/* StageVersion      */
/* ================= */

EXTERN_C int DLL_DECL stage_version(int *majorVersion,
                                    int *minorVersion,
                                    int *majorRelease,
                                    int *minorRelease,
                                    struct stage_options* opts) {
  const char *func = "stage_version";
  int ret=0;
  int rc = -1;
  int saved_serrno = 0;
  std::vector<castor::rh::Response *>respvec;

  try {
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client;
    castor::query::VersionQuery req;
    ret=setDefaultOption(opts);
    client.setOptions(opts);
    client.setAuthorizationId();
    if(ret==-1){free(opts);}

    // Submitting the request
    castor::client::VectorResponseHandler rh(&respvec);
    std::string reqid = client.sendRequest(&req, &rh);

    // Checking the result
    // Parsing the responses which have been stored in the vector
    int nbResponses =  respvec.size();
    if (nbResponses <= 0) {
      castor::exception::Internal e;
      e.getMessage() << "No responses received";
      throw e;
    }

    // Casting the response into a VersionResponse
    castor::query::VersionResponse* vr =
      dynamic_cast<castor::query::VersionResponse*>(respvec[0]);
    if (0 == vr) {
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << "Error in dynamic cast, response was NOT a VersionResponse";
      throw e;
    }
    *majorVersion = vr->majorVersion();
    *minorVersion = vr->minorVersion();
    *majorRelease = vr->majorRelease();
    *minorRelease = vr->minorRelease();
    rc = 0;
  } catch (castor::exception::Communication e) {
    stager_errmsg(func, (e.getMessage().str().c_str()));
    rc = -1;
    saved_serrno = e.code();
  } catch (castor::exception::Exception e) {
    stager_errmsg(func, (e.getMessage().str().c_str()));
    rc = -1;
    saved_serrno = e.code();
  }

  // The responses should be deallocated by the API !
  // Only one entry has been put in the vector
  if (respvec.size() > 0 && 0 != respvec[0]) delete respvec[0];

  serrno = saved_serrno;
  return rc;
}
