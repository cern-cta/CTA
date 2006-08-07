/*
 * $Id: stager_client_api_query.cpp,v 1.24 2006/08/07 07:24:55 sponcec3 Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_query.cpp,v $ $Revision: 1.24 $ $Date: 2006/08/07 07:24:55 $ CERN IT-ADC/CA Benjamin Couturier";
#endif

/* ============== */
/* System headers */
/* ============== */

/* ============= */
/* Local headers */
/* ============= */
#include "errno.h"
#include "u64subr.h"
#include "stager_client_api.h"
#include "stager_admin_api.h"
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/rh/FileQryResponse.hpp"
#include "castor/rh/RequestQueryResponse.hpp"
#include "castor/stager/StageRequestQueryRequest.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/SubRequest.hpp"
#include "stager_client_api_common.h"
#include "stager_client_api.h"
#include "castor/stager/RequestHelper.hpp"
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

  
  char *func = "stage_filequery";
  int ret;
  if (requests == NULL
      || nbreqs <= 0
      || responses == NULL
      || nbresps == NULL) {
    serrno = EINVAL;
    stage_errmsg(func, "Invalid input parameter");
    return -1;
  }

  stage_trace(3, "%s", func);
  
  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());
    client.setOption(NULL);
    castor::stager::StageFileQueryRequest req;
    
    castor::stager::RequestHelper reqh(&req);
    ret=setDefaultOption(opts);
    reqh.setOptions(opts);
    client.setOption(opts);
    if (ret==-1){free(opts);}
    
    // Preparing the requests
    for(int i=0; i<nbreqs; i++) {
      
      if (!(requests[i].param)) {
        serrno = EINVAL;
        stage_errmsg(func, "Parameter in request %d is NULL", i);
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
      stage_errmsg(func, "No responses received");
      return -1;
    }
    

    // Creating the array of file responses
    // Same size as requests as we only do files for the moment
    *responses = (struct stage_filequery_resp *) 
      malloc(sizeof(struct stage_filequery_resp) * nbResponses);

    if (*responses == NULL) {
      serrno = ENOMEM;
      stage_errmsg(func, "Could not allocate memory for responses");
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
          e.getMessage() << "Error in dynamic cast, response was NOT a Response";
          throw e;
        } else {
          (*responses)[i].errorCode = res->errorCode();
          (*responses)[i].errorMessage = strdup(res->errorMessage().c_str());
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
					 struct stage_options* opts) {


   char *func = "stage_requestquery";
   int ret=0;
 
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
    castor::client::BaseClient client(stage_getClientTimeout());
    client.setOption(NULL);
    castor::stager::StageRequestQueryRequest req;
    ret=setDefaultOption(opts);
    castor::stager::RequestHelper reqh(&req);  
    reqh.setOptions(opts);
    client.setOption(opts);
    if(ret==-1){free(opts);}
    // Preparing the requests
    for(int i=0; i<nbreqs; i++) {
      
      if (!(requests[i].param)) {
        serrno = EINVAL;
        stage_errmsg(func, "Parameter in request %d is NULL", i);
        return -1;
      }
      
      castor::stager::QueryParameter *par = new castor::stager::QueryParameter();
      // beware, here the order in enum query_type and RequestQueryType must match
      par->setQueryType((castor::stager::RequestQueryType)(requests[i].type)); 
      par->setValue((const char *)requests[i].param);
      req.addParameters(par);
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
    *responses = (struct stage_requestquery_resp *) 
      malloc(sizeof(struct stage_requestquery_resp) * nbResponses);

    if (*responses == NULL) {
      serrno = ENOMEM;
      stage_errmsg(func, "Could not allocate memory for responses");
      return -1;
    }
    *nbresps = nbResponses;
    
    for (int i=0; i<(int)respvec.size(); i++) {

      // Casting the response into a FileResponse !
      castor::rh::RequestQueryResponse* fr = 
        dynamic_cast<castor::rh::RequestQueryResponse*>(respvec[i]); 
      if (0 == fr) {
        castor::exception::Exception e(SEINTERNAL);
        e.getMessage() << "Error in dynamic cast, response was NOT a file query response";
        throw e;
      }
      // XXX

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



////////////////////////////////////////////////////////////
//    stage_findrequest                                   //
////////////////////////////////////////////////////////////


EXTERN_C int DLL_DECL stage_findrequest(struct stage_query_req *requests,
					int nbreqs,
					struct stage_findrequest_resp **responses,
					int *nbresps,
					struct stage_options* opts){
  serrno = SEOPNOTSUP;
  return -1;
}


////////////////////////////////////////////////////////////
//    stage_translateDiskPoolResponse
////////////////////////////////////////////////////////////

void stage_translateDiskPoolResponse
(castor::query::DiskPoolQueryResponse* fr,
 struct stage_diskpoolquery_resp *response) {
  response->diskPoolName = strdup(fr->diskPoolName().c_str());
  response->freeSpace = fr->freeSpace();
  response->totalSpace = fr->totalSpace();
  response->reservedSpace = fr->reservedSpace();
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
    ds.reservedSpace = dsd->reservedSpace();
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
      fs.reservedSpace = fsd->reservedSpace();
      fs.minFreeSpace = fsd->minFreeSpace();
      fs.maxFreeSpace = fsd->maxFreeSpace();
      fs.status = fsd->status();
    }
  }
}

////////////////////////////////////////////////////////////
//    stage_diskpoolquery                                  //
////////////////////////////////////////////////////////////

EXTERN_C int DLL_DECL stage_diskpoolquery
(char *diskPoolName,
 struct stage_diskpoolquery_resp *response,
 struct stage_options* opts) {

  int ret;
  char *func = "stage_diskpoolquery";
 
  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());
    client.setOption(NULL);
    castor::query::DiskPoolQuery req;
    castor::stager::RequestHelper reqh(&req);
    req.setDiskPoolName(diskPoolName);

    // Dealing with options
    ret = setDefaultOption(opts);
    reqh.setOptions(opts);
    client.setOption(opts);
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
      stage_errmsg(func, "No diskpool found");
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
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    return -1;
  }
  return 0;

}

////////////////////////////////////////////////////////////
//    stage_diskpoolsquery                                  //
////////////////////////////////////////////////////////////

EXTERN_C int DLL_DECL stage_diskpoolsquery
(struct stage_diskpoolquery_resp **responses,
 int *nbresps,
 struct stage_options* opts) {

  int ret;
  char *func = "stage_diskpoolsquery";
 
  try {
    castor::BaseObject::initLog("", castor::SVC_NOMSG);
    
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());
    client.setOption(NULL);
    castor::query::DiskPoolQuery req;
    castor::stager::RequestHelper reqh(&req);

    // Dealing with options
    ret = setDefaultOption(opts);
    reqh.setOptions(opts);
    client.setOption(opts);
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
      stage_errmsg(func, "Could not allocate memory for responses");
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
    stage_errmsg(func, (char *)(e.getMessage().str().c_str()));
    return -1;
  }
  return 0;

}

////////////////////////////////////////////////////////////
//    stage_delete_diskpoolquery_resp                    //
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
//    stage_print_diskpoolsquery_resp                     //
////////////////////////////////////////////////////////////

EXTERN_C void DLL_DECL stage_print_diskpoolquery_resp
(FILE* stream, struct stage_diskpoolquery_resp *response) {
  char freeBuf[21];
  char totalBuf[21];
  char reservedBuf[21];
  char freepBuf[21];
  char reservedpBuf[21];
  if (0 == response) return;
  u64tostru(response->freeSpace, freeBuf, 0);
  u64tostru(response->totalSpace, totalBuf, 0);
  u64tostru(response->reservedSpace, reservedBuf, 0);
  if (0 == response->totalSpace) {
    strncpy(freepBuf, " -", 3);
    strncpy(reservedpBuf, " -", 3);
  } else {
    snprintf(freepBuf, 3, "%2lld", (100*response->freeSpace)/response->totalSpace);
    snprintf(reservedpBuf, 3, "%2lld", (100*response->reservedSpace)/response->totalSpace);
  }
  fprintf(stream, "POOL %-16s CAPACITY %-10s FREE %7s(%s\%)  RESERVED %7s(%-2s\%)\n",
  response->diskPoolName, totalBuf, freeBuf,
  freepBuf, reservedBuf, reservedpBuf);
  for (int i = 0; i < response->nbDiskServers; i++) {
    struct stage_diskServerDescription& dsd = response->diskServers[i];
    u64tostru(dsd.freeSpace, freeBuf, 0);
    u64tostru(dsd.totalSpace, totalBuf, 0);
    u64tostru(dsd.reservedSpace, reservedBuf, 0);
    if (0 == dsd.totalSpace) {
      strncpy(freepBuf, " -", 3);
      strncpy(reservedpBuf, " -", 3);
    } else {
      snprintf(freepBuf, 3, "%2lld", (100*dsd.freeSpace)/dsd.totalSpace);
      snprintf(reservedpBuf, 3, "%2lld", (100*dsd.reservedSpace)/dsd.totalSpace);
    }
    fprintf(stream, "  DiskServer %-16s %-23s CAPACITY %-10s FREE %7s(%-2s\%)  RESERVED %7s(%-2s\%)\n",
    dsd.name,
    stage_diskServerStatusName(dsd.status),
    totalBuf, freeBuf, freepBuf, reservedBuf, reservedpBuf);
    fprintf(stream, "     %-33s %-23s %-10s FREE          RESERVED      GCBOUNDS\n", "FileSystems", "STATUS", "CAPACITY");
    for (int j = 0; j < dsd.nbFileSystems; j++) {
      struct stage_fileSystemDescription& fsd = dsd.fileSystems[j];
      u64tostru(fsd.freeSpace, freeBuf, 0);
      u64tostru(fsd.totalSpace, totalBuf, 0);
      u64tostru(fsd.reservedSpace, reservedBuf, 0);
      if (0 == fsd.totalSpace) {
        strncpy(freepBuf, " -", 3);
        strncpy(reservedpBuf, " -", 3);
      } else {
        snprintf(freepBuf, 3, "%2lld", (100*fsd.freeSpace)/fsd.totalSpace);
        snprintf(reservedpBuf, 3, "%2lld", (100*fsd.reservedSpace)/fsd.totalSpace);
      }
      fprintf(stream, "     %-33s %-23s %-10s %7s(%-2s\%) %7s(%-2s\%)   %4.2f, %4.2f\n",
      fsd.mountPoint,
      stage_fileSystemStatusName(fsd.status),
      totalBuf, freeBuf, freepBuf,
      reservedBuf, reservedpBuf,
      fsd.minFreeSpace, fsd.maxFreeSpace);
    }
  }
}
