/*
 * $Id: stager_client_api_query.cpp,v 1.41 2009/07/13 06:22:08 waldron Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/* ============== */
/* System headers */
/* ============== */

/* ============= */
/* Local headers */
/* ============= */
#include "errno.h"
#include "u64subr.h"
#include "stager_client_api.h"
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/rh/FileQryResponse.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/SubRequest.hpp"
#include "stager_client_api_common.hpp"
#include "stager_client_api.h"
#include "castor/query/DiskPoolQuery.hpp"
#include "castor/query/DiskPoolQueryResponse.hpp"
#include "castor/query/DiskServerDescription.hpp"
#include "castor/query/FileSystemDescription.hpp"

/* ================= */
/* External routines */
/* ================= */


////////////////////////////////////////////////////////////
//    stage_filequery                                     //
////////////////////////////////////////////////////////////

EXTERN_C int DLL_DECL stage_filequery(struct stage_query_req *requests,
				      int nbreqs,
				      struct stage_filequery_resp **responses,
				      int *nbresps,
				      struct stage_options* opts){

  const char *func = "stage_filequery";
  int ret;
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
    castor::stager::StageFileQueryRequest req;

    ret=setDefaultOption(opts);
    client.setOptions(opts);
    client.setAuthorizationId();
    if (ret==-1){free(opts);}

    // Preparing the requests
    for(int i=0; i<nbreqs; i++) {

      if (!(requests[i].param)) {
        serrno = EINVAL;
        stager_errmsg(func, "Parameter in request %d is NULL", i);
        return -1;
      }

      castor::stager::QueryParameter *par = new castor::stager::QueryParameter();
      par->setQueryType((castor::stager::RequestQueryType)(requests[i].type));
      par->setValue((const char *)requests[i].param);
      par->setQuery(&req);
      req.addParameters(par);

      stage_trace(3, "%s type=%d param=%s",
		  func, requests[i].type, requests[i].param);
    }

    // Using the VectorResponseHandler which stores everything in
    // a vector. BEWARE, the responses must be de-allocated afterwards
    std::vector<castor::rh::Response*>respvec;
    castor::client::VectorResponseHandler rh(&respvec);
    client.sendRequest(&req, &rh);

    // Parsing the responses which have been stored in the vector
    int nbResponses =  respvec.size();

    if (nbResponses <= 0) {
      // We got no replies, this is not normal !
      serrno = SEINTERNAL;
      stager_errmsg(func, "No responses received");
      return -1;
    }


    // Creating the array of file responses
    // Same size as requests as we only do files for the moment
    *responses = (struct stage_filequery_resp *)
      malloc(sizeof(struct stage_filequery_resp) * nbResponses);

    if (*responses == NULL) {
      serrno = ENOMEM;
      stager_errmsg(func, "Could not allocate memory for responses");
      return -1;
    }
    *nbresps = nbResponses;

    for (int i=0; i<(int)respvec.size(); i++) {

      // Casting the response into a FileResponse
      castor::rh::FileQryResponse* fr =
        dynamic_cast<castor::rh::FileQryResponse*>(respvec[i]);
      if (0 == fr) {
        // try a simple Response
        castor::rh::Response* res =
          dynamic_cast<castor::rh::Response*>(respvec[i]);
        if (0 == res) {
          // Not even a Response !
          castor::exception::Exception e(SEINTERNAL);
          e.getMessage() << "Error in dynamic cast, response was NOT a Response (type was "
                         << respvec[i]->type() << ")";
          throw e;
        } else {
          (*responses)[i].errorCode = res->errorCode();
          (*responses)[i].errorMessage = strdup(res->errorMessage().c_str());
          // wipe out other unused pointers
          (*responses)[i].filename = (*responses)[i].castorfilename = 
            (*responses)[i].diskserver = (*responses)[i].poolname = 0;
        }
      } else {
        (*responses)[i].errorCode = fr->errorCode();
        (*responses)[i].errorMessage = strdup(fr->errorMessage().c_str());
        (*responses)[i].filename = strdup(fr->fileName().c_str());
        (*responses)[i].castorfilename = strdup(fr->castorFileName().c_str());
        (*responses)[i].fileid = fr->fileId();
        (*responses)[i].status = fr->status();
        (*responses)[i].size = fr->size();
        (*responses)[i].diskserver = strdup(fr->diskServer().c_str());
        (*responses)[i].poolname = strdup(fr->poolName().c_str());
        (*responses)[i].creationTime = (time_t)fr->creationTime();
        (*responses)[i].accessTime = (time_t)fr->accessTime();
        (*responses)[i].nbAccesses = fr->nbAccesses();
      }

      // The responses should be deallocated by the API !
      delete respvec[i];
    } // for

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stager_errmsg(func, (e.getMessage().str().c_str()));
    return -1;
  }
  return 0;
}


////////////////////////////////////////////////////////////
//    stage_translateDiskPoolResponse                     //
////////////////////////////////////////////////////////////

void stage_translateDiskPoolResponse
(castor::query::DiskPoolQueryResponse* fr,
 struct stage_diskpoolquery_resp *response) {
  response->diskPoolName = strdup(fr->diskPoolName().c_str());
  response->freeSpace = fr->freeSpace();
  response->totalSpace = fr->totalSpace();
  response->reservedSpace = 0;
  int nbDiskServers = fr->diskServers().size();
  response->nbDiskServers = nbDiskServers;
  response->diskServers = (struct stage_diskServerDescription*)
    malloc(sizeof(struct stage_diskServerDescription) * nbDiskServers);
  for (int i = 0; i < nbDiskServers; i++) {
    castor::query::DiskServerDescription *dsd = fr->diskServers()[i];
    struct stage_diskServerDescription &ds = response->diskServers[i];
    ds.name = strdup(dsd->name().c_str());
    ds.status = dsd->status();
    ds.freeSpace = dsd->freeSpace();
    ds.totalSpace = dsd->totalSpace();
    ds.reservedSpace = 0;
    int nbFileSystems = dsd->fileSystems().size();
    ds.nbFileSystems = nbFileSystems;
    ds.fileSystems = (struct stage_fileSystemDescription*)
      malloc(sizeof(struct stage_fileSystemDescription) * nbFileSystems);
    for (int j = 0; j < nbFileSystems; j++) {
      castor::query::FileSystemDescription *fsd = dsd->fileSystems()[j];
      struct stage_fileSystemDescription &fs = ds.fileSystems[j];
      fs.mountPoint = strdup(fsd->mountPoint().c_str());
      fs.freeSpace = fsd->freeSpace();
      fs.totalSpace = fsd->totalSpace();
      fs.reservedSpace = 0;
      fs.minFreeSpace = fsd->minFreeSpace();
      fs.maxFreeSpace = fsd->maxFreeSpace();
      fs.status = fsd->status();
    }
  }
}


////////////////////////////////////////////////////////////
//    stage_diskpoolquery                                 //
////////////////////////////////////////////////////////////

EXTERN_C int DLL_DECL stage_diskpoolquery
(char *diskPoolName,
 struct stage_diskpoolquery_resp *response,
 struct stage_options* opts) {

  int ret;
  const char *func = "stage_diskpoolquery";

  try {
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());
    castor::query::DiskPoolQuery req;
    req.setDiskPoolName(diskPoolName);

    // Dealing with options
    ret = setDefaultOption(opts);
    client.setOptions(opts);
    client.setAuthorizationId();
    if (-1 == ret) { free(opts); }

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
      stager_errmsg(func, "No diskpool found");
      return -1;
    }

    // Check for error
    if (respvec[0]->errorCode() != 0) {
      castor::exception::Exception e(respvec[0]->errorCode());
      e.getMessage() << respvec[0]->errorMessage();
      delete respvec[0];
      // throw exception
      throw e;
    }

    // Casting the response into a DiskPoolQueryResponse
    castor::query::DiskPoolQueryResponse* fr =
      dynamic_cast<castor::query::DiskPoolQueryResponse*>(respvec[0]);
    if (0 == fr) {
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << "Error in dynamic cast, response was NOT a diskpool query response."
		     << " Type was " << respvec[0]->type();
      throw e;
    }

    // build C response from C++ one
    stage_translateDiskPoolResponse(fr, response);

    // Cleanup memory
    delete respvec[0];

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stager_errmsg(func, (e.getMessage().str().c_str()));
    return -1;
  }
  return 0;
}


////////////////////////////////////////////////////////////
//    stage_diskpoolsquery                                //
////////////////////////////////////////////////////////////

EXTERN_C int DLL_DECL stage_diskpoolsquery
(struct stage_diskpoolquery_resp **responses,
 int *nbresps,
 struct stage_options* opts) {

  int ret;
  const char *func = "stage_diskpoolsquery";

  try {
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());
    castor::query::DiskPoolQuery req;

    // Dealing with options
    ret = setDefaultOption(opts);
    client.setOptions(opts);
    client.setAuthorizationId();
    if (-1 == ret) { free(opts); }

    // Using the VectorResponseHandler which stores everything in
    // A vector. BEWARE, the responses must be de-allocated afterwards
    std::vector<castor::rh::Response *>respvec;
    castor::client::VectorResponseHandler rh(&respvec);
    client.sendRequest(&req, &rh);

    // Creating the array of file responses
    *nbresps =  respvec.size();
    // Same size as requests as we only do files for the moment
    *responses = (struct stage_diskpoolquery_resp*)
      malloc(sizeof(struct stage_diskpoolquery_resp) * (*nbresps));
    if (*nbresps <= 0) {
      // We found no diskpool
      return 0;
    }
    if (*responses == NULL) {
      serrno = ENOMEM;
      stager_errmsg(func, "Could not allocate memory for responses");
      return -1;
    }

    // Loop on the responses
    for (int i = 0; i < *nbresps; i++) {

      if (respvec[i]->errorCode() != 0) {
	castor::exception::Exception e(respvec[i]->errorCode());
	e.getMessage() << respvec[i]->errorMessage();
        // cleanup previous responses
        for (int j = 0; j < i; j++) {
          stage_delete_diskpoolquery_resp(&((*responses)[j]));
        }
        // cleanup the list
        free(*responses);
	// free remaining C++ responses
	for (int j = i; j < *nbresps; j++) {
	  delete respvec[j];
	}
	// throw exception
	throw e;
      }

      // Casting the response into a DiskPoolQueryResponse
      castor::query::DiskPoolQueryResponse* fr =
        dynamic_cast<castor::query::DiskPoolQueryResponse*>(respvec[i]);
      if (0 == fr) {
        // cleanup previous responses
        for (int j = 0; j < i; j++) {
          stage_delete_diskpoolquery_resp(&((*responses)[j]));
        }
        // cleanup the list
        free(*responses);
	// free remaining C++ responses
	for (int j = i; j < *nbresps; j++) {
	  delete respvec[j];
	}
        castor::exception::Exception e(SEINTERNAL);
        e.getMessage() << "Error in dynamic cast, response was NOT a diskpool query response."
		       << " Type was " << respvec[i]->type();
        throw e;
      }

      // build C response from C++ one
      stage_translateDiskPoolResponse(fr, &((*responses)[i]));

      // Cleenup memory
      delete respvec[i];
    }

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stager_errmsg(func, (e.getMessage().str().c_str()));
    return -1;
  }
  return 0;
}


////////////////////////////////////////////////////////////
//    stage_delete_diskpoolquery_resp                     //
////////////////////////////////////////////////////////////

EXTERN_C void DLL_DECL stage_delete_diskpoolquery_resp
(struct stage_diskpoolquery_resp *response) {
  if (0 == response) return;
  free(response->diskPoolName);
  for (int i = 0; i < response->nbDiskServers; i++) {
    free(response->diskServers[i].name);
    for (int j = 0; j < response->diskServers[i].nbFileSystems; j++) {
      free(response->diskServers[i].fileSystems[j].mountPoint);
    }
    free(response->diskServers[i].fileSystems);
  }
  free(response->diskServers);
}


////////////////////////////////////////////////////////////
//    stage_print_diskpoolquery_resp                     //
////////////////////////////////////////////////////////////

void printSizeToBuf(char* buf, unsigned long long int size, int siflag) {
  if (siflag & (HUMANREADABLE | SIUNITS)) {
    if (siflag & SIUNITS) {
      u64tostrsi(size, buf, 0);
    } else {
      u64tostru(size, buf, 0);
    }
  } else {
    sprintf(buf, "%llu", size);
  }
}

EXTERN_C void DLL_DECL stage_print_diskpoolquery_resp
(FILE* stream, struct stage_diskpoolquery_resp *response, int siflag) {
  char freeBuf[21];
  char totalBuf[21];
  char freepBuf[21];
  if (0 == response) return;
  printSizeToBuf(freeBuf, response->freeSpace, siflag);
  printSizeToBuf(totalBuf, response->totalSpace, siflag);
  if (0 == response->totalSpace) {
    strncpy(freepBuf, " -", 3);
  } else {
    snprintf(freepBuf, 3, "%2lld", (100*response->freeSpace)/response->totalSpace);
  }
  fprintf(stream, "POOL %-16s CAPACITY %-10s FREE %7s(%s%%)\n",
  response->diskPoolName, totalBuf, freeBuf,
  freepBuf);
  for (int i = 0; i < response->nbDiskServers; i++) {
    struct stage_diskServerDescription& dsd = response->diskServers[i];
    printSizeToBuf(freeBuf, response->freeSpace, siflag);
    printSizeToBuf(totalBuf, response->totalSpace, siflag);
    if (0 == dsd.totalSpace) {
      strncpy(freepBuf, " -", 3);
    } else {
      snprintf(freepBuf, 3, "%2lld", (100*dsd.freeSpace)/dsd.totalSpace);
    }
    fprintf(stream, "  DiskServer %-16s %-23s CAPACITY %-10s FREE %7s(%-2s%%)\n",
    dsd.name,
    stage_diskServerStatusName(dsd.status),
    totalBuf, freeBuf, freepBuf);
    fprintf(stream, "     %-25s %-23s %-16s          FREE           GCBOUNDS\n", "FileSystems", "STATUS", "CAPACITY");
    for (int j = 0; j < dsd.nbFileSystems; j++) {
      struct stage_fileSystemDescription& fsd = dsd.fileSystems[j];
      printSizeToBuf(freeBuf, response->freeSpace, siflag);
      printSizeToBuf(totalBuf, response->totalSpace, siflag);
      if (0 == fsd.totalSpace) {
        strncpy(freepBuf, " -", 3);
      } else {
        snprintf(freepBuf, 3, "%2lld", (100*fsd.freeSpace)/fsd.totalSpace);
      }
      fprintf(stream, "     %-25s %-23s %-16s %16s(%-2s%%) %4.2f, %4.2f\n",
      fsd.mountPoint,
      stage_fileSystemStatusName(fsd.status),
      totalBuf, freeBuf, freepBuf,
      fsd.minFreeSpace, fsd.maxFreeSpace);
    }
  }
}
